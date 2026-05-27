// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2012 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

//go:build go1.27

package mysql

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
)

var (
	_ driver.RowsColumnScanner = (*textRows)(nil)
	_ driver.RowsColumnScanner = (*binaryRows)(nil)
)

// readRowPacket handles the common row/error/EOF packet framing. Invalidate the
// previous row even on failure, and retain the column slice for the next row.
func (rows *mysqlRows) readRowPacket(binary bool) ([]byte, error) {
	rows.rawCols = rows.rawCols[:0]
	mc := rows.mc
	if mc == nil {
		return nil, io.EOF
	}
	if err := mc.error(); err != nil {
		return nil, err
	}
	if rows.rs.done {
		return nil, io.EOF
	}
	data, err := mc.readPacket()
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, ErrMalformPkt
	}
	if data[0] == iEOF && (binary || len(data) <= 0xffffff) {
		statusPos := 3
		if mc.capabilities&clientDeprecateEOF != 0 {
			// Skip affected_rows and last_insert_id, both length-encoded integers.
			statusPos = 1
			for range 2 {
				if statusPos >= len(data) {
					return nil, ErrMalformPkt
				}
				n := 1
				switch data[statusPos] {
				case 0xfc:
					n = 3
				case 0xfd:
					n = 4
				case 0xfe:
					n = 9
				}
				statusPos += n
			}
		}
		if len(data) < statusPos+2 {
			return nil, ErrMalformPkt
		}
		mc.status = readStatus(data[statusPos:])
		rows.rs.done = true
		if !rows.HasNextResultSet() {
			rows.mc = nil
		}
		return nil, io.EOF
	}
	if data[0] == iERR || (binary && data[0] != iOK) {
		rows.mc = nil
		return nil, mc.handleErrorPacket(data)
	}
	n := len(rows.rs.columns)
	if cap(rows.rawCols) < n {
		rows.rawCols = make([][]byte, n)
	} else {
		rows.rawCols = rows.rawCols[:n]
	}
	return data, nil
}

// readColumnBytes bounds-checks both the length prefix and the payload before
// slicing. A non-NULL empty value must remain distinct from nil.
func readColumnBytes(data []byte) (raw, rest []byte, err error) {
	if len(data) == 0 {
		return nil, nil, ErrMalformPkt
	}
	prefix := 1
	switch data[0] {
	case 0xfc:
		prefix = 3
	case 0xfd:
		prefix = 4
	case 0xfe:
		prefix = 9
	case 0xff:
		return nil, nil, ErrMalformPkt
	}
	if len(data) < prefix {
		return nil, nil, ErrMalformPkt
	}
	n, null, _ := readLengthEncodedInteger(data)
	data = data[prefix:]
	if null {
		return nil, data, nil
	}
	if n > uint64(len(data)) {
		return nil, nil, ErrMalformPkt
	}
	return data[:int(n):int(n)], data[int(n):], nil
}

func (rows *textRows) NextRow() error {
	data, err := rows.readRowPacket(false)
	if err != nil {
		return err
	}
	for i := range rows.rawCols {
		rows.rawCols[i], data, err = readColumnBytes(data)
		if err != nil {
			rows.rawCols = rows.rawCols[:0]
			return err
		}
	}
	if len(data) != 0 {
		rows.rawCols = rows.rawCols[:0]
		return ErrMalformPkt
	}
	return nil
}

func (rows *binaryRows) NextRow() error {
	data, err := rows.readRowPacket(true)
	if err != nil {
		return err
	}
	// Two reserved bits precede the column bits in the NULL bitmap.
	pos := 1 + (len(rows.rawCols)+7+2)/8
	if len(data) < pos {
		rows.rawCols = rows.rawCols[:0]
		return ErrMalformPkt
	}
	nullMask := data[1:pos]
	data = data[pos:]
	for i, col := range rows.rs.columns {
		if nullMask[(i+2)/8]&(1<<uint((i+2)%8)) != 0 || col.fieldType == fieldTypeNULL {
			rows.rawCols[i] = nil
			continue
		}
		size := 0
		switch col.fieldType {
		case fieldTypeTiny:
			size = 1
		case fieldTypeShort, fieldTypeYear:
			size = 2
		case fieldTypeInt24, fieldTypeLong, fieldTypeFloat:
			size = 4
		case fieldTypeLongLong, fieldTypeDouble:
			size = 8
		case fieldTypeDecimal, fieldTypeNewDecimal, fieldTypeVarChar,
			fieldTypeBit, fieldTypeEnum, fieldTypeSet, fieldTypeTinyBLOB,
			fieldTypeMediumBLOB, fieldTypeLongBLOB, fieldTypeBLOB,
			fieldTypeVarString, fieldTypeString, fieldTypeGeometry, fieldTypeJSON,
			fieldTypeVector, fieldTypeDate, fieldTypeNewDate, fieldTypeTime,
			fieldTypeTimestamp, fieldTypeDateTime:
			rows.rawCols[i], data, err = readColumnBytes(data)
			if err != nil {
				rows.rawCols = rows.rawCols[:0]
				return err
			}
			continue
		default:
			rows.rawCols = rows.rawCols[:0]
			return fmt.Errorf("unknown field type %d", col.fieldType)
		}
		if len(data) < size {
			rows.rawCols = rows.rawCols[:0]
			return ErrMalformPkt
		}
		rows.rawCols[i], data = data[:size:size], data[size:]
	}
	if len(data) != 0 {
		rows.rawCols = rows.rawCols[:0]
		return ErrMalformPkt
	}
	return nil
}

// Assign matching scalar types before boxing the value for ConvertAssign.
// Other conversions (including Scanner and named types) belong to database/sql.
func scanColumnValue[T any](ctx driver.ScanContext, dest any, value T) error {
	if d, ok := dest.(*T); ok {
		if d == nil {
			return errors.New("destination pointer is nil")
		}
		*d = value
		return nil
	}
	return sql.ConvertAssign(ctx, dest, value)
}

func scanColumnBytes(ctx driver.ScanContext, dest any, raw []byte) error {
	switch d := dest.(type) {
	case *string:
		if d != nil {
			*d = string(raw)
			return nil
		}
	case *[]byte:
		// Each Scan gives the caller an independent copy, including empty values.
		if d != nil {
			*d = bytes.Clone(raw)
			return nil
		}
	case *sql.RawBytes:
		if d != nil {
			*d = raw
			return nil
		}
	}
	return sql.ConvertAssign(ctx, dest, raw)
}

func (rows *mysqlRows) scanColumnInt64(ctx driver.ScanContext, i int, dest any, value int64) error {
	if rows.tinyInt1IsBool(i) {
		return scanColumnValue(ctx, dest, value != 0)
	}
	if d, ok := dest.(*int); ok && d != nil && int64(int(value)) == value {
		*d = int(value)
		return nil
	}
	return scanColumnValue(ctx, dest, value)
}

func (rows *textRows) ScanColumn(ctx driver.ScanContext, i int, dest any) error {
	if i < 0 || i >= len(rows.rawCols) {
		return fmt.Errorf("mysql: column index %d out of range [0, %d)", i, len(rows.rawCols))
	}
	raw := rows.rawCols[i]
	if raw == nil {
		return sql.ConvertAssign(ctx, dest, nil)
	}
	col := rows.rs.columns[i]
	switch col.fieldType {
	case fieldTypeTimestamp, fieldTypeDateTime, fieldTypeDate, fieldTypeNewDate:
		if rows.mc.parseTime {
			value, err := parseDateTime(raw, rows.mc.cfg.Loc)
			if err != nil {
				return err
			}
			return scanColumnValue(ctx, dest, value)
		}
	case fieldTypeTiny, fieldTypeShort, fieldTypeInt24, fieldTypeYear, fieldTypeLong:
		value, err := strconv.ParseInt(string(raw), 10, 64)
		if err != nil {
			return err
		}
		return rows.scanColumnInt64(ctx, i, dest, value)
	case fieldTypeLongLong:
		if col.flags&flagUnsigned != 0 {
			value, err := strconv.ParseUint(string(raw), 10, 64)
			if err != nil {
				return err
			}
			return scanColumnValue(ctx, dest, value)
		}
		value, err := strconv.ParseInt(string(raw), 10, 64)
		if err != nil {
			return err
		}
		return rows.scanColumnInt64(ctx, i, dest, value)
	case fieldTypeFloat:
		value, err := strconv.ParseFloat(string(raw), 32)
		if err != nil {
			return err
		}
		return scanColumnValue(ctx, dest, float32(value))
	case fieldTypeDouble:
		value, err := strconv.ParseFloat(string(raw), 64)
		if err != nil {
			return err
		}
		return scanColumnValue(ctx, dest, value)
	}
	return scanColumnBytes(ctx, dest, raw)
}

func (rows *binaryRows) ScanColumn(ctx driver.ScanContext, i int, dest any) error {
	if i < 0 || i >= len(rows.rawCols) {
		return fmt.Errorf("mysql: column index %d out of range [0, %d)", i, len(rows.rawCols))
	}
	raw := rows.rawCols[i]
	if raw == nil {
		return sql.ConvertAssign(ctx, dest, nil)
	}

	col := rows.rs.columns[i]
	var value int64
	switch col.fieldType {
	case fieldTypeTiny:
		value = int64(int8(raw[0]))
		if col.flags&flagUnsigned != 0 {
			value = int64(raw[0])
		}
	case fieldTypeShort, fieldTypeYear:
		n := binary.LittleEndian.Uint16(raw)
		value = int64(int16(n))
		if col.flags&flagUnsigned != 0 {
			value = int64(n)
		}
	case fieldTypeInt24, fieldTypeLong:
		n := binary.LittleEndian.Uint32(raw)
		value = int64(int32(n))
		if col.flags&flagUnsigned != 0 {
			value = int64(n)
		}
	case fieldTypeLongLong:
		n := binary.LittleEndian.Uint64(raw)
		if col.flags&flagUnsigned != 0 {
			if d, ok := dest.(*uint64); ok && d != nil {
				*d = n
				return nil
			}
			if n > math.MaxInt64 {
				// Preserve Next's []byte source for Scanner and interface destinations.
				return scanColumnBytes(ctx, dest, uint64ToString(n))
			}
		}
		value = int64(n)

	case fieldTypeFloat:
		return scanColumnValue(ctx, dest, math.Float32frombits(binary.LittleEndian.Uint32(raw)))

	case fieldTypeDouble:
		return scanColumnValue(ctx, dest, math.Float64frombits(binary.LittleEndian.Uint64(raw)))

	case fieldTypeDecimal, fieldTypeNewDecimal, fieldTypeVarChar,
		fieldTypeBit, fieldTypeEnum, fieldTypeSet, fieldTypeTinyBLOB,
		fieldTypeMediumBLOB, fieldTypeLongBLOB, fieldTypeBLOB,
		fieldTypeVarString, fieldTypeString, fieldTypeGeometry, fieldTypeJSON,
		fieldTypeVector:
		return scanColumnBytes(ctx, dest, raw)

	case fieldTypeDate, fieldTypeNewDate, fieldTypeTimestamp, fieldTypeDateTime, fieldTypeTime:
		if col.fieldType != fieldTypeTime && rows.mc.parseTime {
			value, err := parseBinaryDateTime(uint64(len(raw)), raw, rows.mc.cfg.Loc)
			if err != nil {
				return err
			}
			return scanColumnValue(ctx, dest, value)
		}
		length := uint8(19)
		switch col.fieldType {
		case fieldTypeDate, fieldTypeNewDate:
			length = 10
		case fieldTypeTime:
			length = 8
		}
		if length != 10 {
			switch col.decimals {
			case 0, 0x1f:
			case 1, 2, 3, 4, 5, 6:
				length += 1 + col.decimals
			default:
				return fmt.Errorf("protocol error, illegal decimals value %d", col.decimals)
			}
		}
		var value driver.Value
		var err error
		if col.fieldType == fieldTypeTime {
			value, err = formatBinaryTime(raw, length)
		} else {
			value, err = formatBinaryDateTime(raw, length)
		}
		if err != nil {
			return err
		}
		return sql.ConvertAssign(ctx, dest, value)

	default:
		return fmt.Errorf("unknown field type %d", col.fieldType)
	}
	return rows.scanColumnInt64(ctx, i, dest, value)
}

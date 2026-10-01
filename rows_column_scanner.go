// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2026 The Go-MySQL-Driver Authors. All rights reserved.
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
	"math"
	"strconv"
)

var (
	_ driver.RowsColumnScanner = (*textRows)(nil)
	_ driver.RowsColumnScanner = (*binaryRows)(nil)
)

func (rows *mysqlRows) prepareRawColumns() {
	n := len(rows.rs.columns)
	if cap(rows.rawCols) < n {
		rows.rawCols = make([][]byte, n)
	} else {
		rows.rawCols = rows.rawCols[:n]
	}
}

func (rows *textRows) NextRow() error {
	data, err := rows.readRowPacket(false)
	if err != nil {
		return err
	}
	rows.prepareRawColumns()
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
	reader, err := newBinaryRowReader(data, len(rows.rs.columns))
	if err != nil {
		return err
	}
	rows.prepareRawColumns()
	for i := range rows.rs.columns {
		fieldType := rows.rs.columns[i].fieldType
		if reader.isNull(i, fieldType) {
			rows.rawCols[i] = nil
			continue
		}
		raw, fixed := reader.readFixedColumn(fieldType)
		if !fixed {
			raw, err = reader.readVariableColumn(fieldType)
			if err != nil {
				rows.rawCols = rows.rawCols[:0]
				return err
			}
		}
		rows.rawCols[i] = raw
	}
	if len(reader.data) != 0 {
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
		value, err := formatBinaryColumnDateTime(col, raw)
		if err != nil {
			return err
		}
		return sql.ConvertAssign(ctx, dest, value)

	default:
		return fmt.Errorf("unknown field type %d", col.fieldType)
	}
	return rows.scanColumnInt64(ctx, i, dest, value)
}

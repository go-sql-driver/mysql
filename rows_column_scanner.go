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
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strconv"
)

// NextRow implements driver.RowsColumnScanner.
// It reads the next row from the MySQL text protocol and stores raw per-column
// byte slices (subslices of the packet buffer) in rows.rawCols, without
// allocating any intermediate []driver.Value.
func (rows *textRows) NextRow() error {
	mc := rows.mc
	if mc == nil {
		return io.EOF
	}
	if err := mc.error(); err != nil {
		return err
	}
	if rows.rs.done {
		return io.EOF
	}

	data, err := mc.readPacket()
	if err != nil {
		return err
	}

	// EOF Packet.
	// Text row packets may start with a LengthEncodedString; 0xFE can mean a
	// string larger than 0xffffff, so we bound-check the length.
	// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_dt_integers.html#sect_protocol_basic_dt_int_le
	if data[0] == iEOF && len(data) <= 0xffffff {
		if mc.capabilities&clientDeprecateEOF == 0 {
			// Deprecated EOF packet
			mc.status = readStatus(data[3:])
		} else {
			// OK packet with 0xFE header
			_, _, n := readLengthEncodedInteger(data[1:])
			_, _, m := readLengthEncodedInteger(data[1+n:])
			mc.status = readStatus(data[1+n+m:])
		}
		rows.rs.done = true
		if !rows.HasNextResultSet() {
			rows.mc = nil
		}
		return io.EOF
	}
	if data[0] == iERR {
		rows.mc = nil
		return mc.handleErrorPacket(data)
	}

	// Extract raw per-column byte slices. Each slice is a subslice of the
	// packet buffer — no extra allocation per column.
	ncols := len(rows.rs.columns)
	if len(rows.rawCols) != ncols {
		rows.rawCols = make([][]byte, ncols)
	}

	var n int
	var isNull bool
	pos := 0
	for i := range rows.rawCols {
		rows.rawCols[i], isNull, n, err = readLengthEncodedString(data[pos:])
		pos += n
		if err != nil {
			return err
		}
		if isNull {
			rows.rawCols[i] = nil
		}
	}
	return nil
}

// ScanColumn implements driver.RowsColumnScanner.
// It converts the raw text-protocol bytes for column i directly into dest,
// without any intermediate []driver.Value allocation.
func (rows *textRows) ScanColumn(scanCtx driver.ScanContext, i int, dest any) error {
	if i < 0 || i >= len(rows.rawCols) {
		return fmt.Errorf("mysql: column index %d out of range [0, %d)", i, len(rows.rawCols))
	}
	raw := rows.rawCols[i]
	if raw == nil {
		return sql.ConvertAssign(scanCtx, dest, nil)
	}

	// Fast paths for byte destinations: avoid an intermediate string/value.
	switch d := dest.(type) {
	case *[]byte:
		*d = append((*d)[:0], raw...)
		return nil
	case *sql.RawBytes:
		*d = raw // zero-copy; caller must not retain beyond the next Next/NextRow call
		return nil
	}

	// Parse raw bytes to the canonical driver.Value for this column type,
	// then delegate the final assignment to sql.ConvertAssign.
	col := rows.rs.columns[i]
	var val driver.Value
	var err error
	switch col.fieldType {
	case fieldTypeTimestamp, fieldTypeDateTime, fieldTypeDate, fieldTypeNewDate:
		if rows.mc.parseTime {
			val, err = parseDateTime(raw, rows.mc.cfg.Loc)
		} else {
			val = string(raw)
		}
	case fieldTypeTiny, fieldTypeShort, fieldTypeInt24, fieldTypeYear, fieldTypeLong:
		val, err = strconv.ParseInt(string(raw), 10, 64)
	case fieldTypeLongLong:
		if col.flags&flagUnsigned != 0 {
			val, err = strconv.ParseUint(string(raw), 10, 64)
		} else {
			val, err = strconv.ParseInt(string(raw), 10, 64)
		}
	case fieldTypeFloat:
		var f float64
		f, err = strconv.ParseFloat(string(raw), 32)
		val = float32(f)
	case fieldTypeDouble:
		val, err = strconv.ParseFloat(string(raw), 64)
	default:
		val = string(raw)
	}
	if err != nil {
		return err
	}
	return sql.ConvertAssign(scanCtx, dest, val)
}

// NextRow implements driver.RowsColumnScanner.
// It reads the next row from the MySQL binary protocol and stores raw per-column
// byte slices (subslices of the packet buffer) in rows.rawCols, without
// allocating any intermediate []driver.Value.
func (rows *binaryRows) NextRow() error {
	mc := rows.mc
	if mc == nil {
		return io.EOF
	}
	if err := mc.error(); err != nil {
		return err
	}

	data, err := mc.readPacket()
	if err != nil {
		return err
	}

	// Packet indicator [1 byte].
	if data[0] != iOK {
		if data[0] == iEOF {
			if mc.capabilities&clientDeprecateEOF == 0 {
				// EOF packet
				mc.status = readStatus(data[3:])
			} else {
				// OK packet with 0xFE header
				_, _, n := readLengthEncodedInteger(data[1:])
				_, _, m := readLengthEncodedInteger(data[1+n:])
				mc.status = readStatus(data[1+n+m:])
			}
			rows.rs.done = true
			if !rows.HasNextResultSet() {
				rows.mc = nil
			}
			return io.EOF
		}
		rows.mc = nil
		return mc.handleErrorPacket(data)
	}

	ncols := len(rows.rs.columns)
	if len(rows.rawCols) != ncols {
		rows.rawCols = make([][]byte, ncols)
	}

	// NULL-bitmap: ceil((ncols + 2) / 8) bytes, starting at data[1].
	// The binary protocol reserves bits 0 and 1, so column i maps to bit i+2.
	pos := 1 + (ncols+7+2)>>3
	nullMask := data[1:pos]

	for i := range rows.rawCols {
		// Column i is NULL when bit i+2 of the null bitmap is set.
		if ((nullMask[(i+2)>>3] >> uint((i+2)&7)) & 1) == 1 {
			rows.rawCols[i] = nil
			continue
		}

		switch rows.rs.columns[i].fieldType {
		case fieldTypeNULL:
			rows.rawCols[i] = nil

		case fieldTypeTiny:
			rows.rawCols[i] = data[pos : pos+1]
			pos++

		case fieldTypeShort, fieldTypeYear:
			rows.rawCols[i] = data[pos : pos+2]
			pos += 2

		case fieldTypeInt24, fieldTypeLong:
			rows.rawCols[i] = data[pos : pos+4]
			pos += 4

		case fieldTypeLongLong:
			rows.rawCols[i] = data[pos : pos+8]
			pos += 8

		case fieldTypeFloat:
			rows.rawCols[i] = data[pos : pos+4]
			pos += 4

		case fieldTypeDouble:
			rows.rawCols[i] = data[pos : pos+8]
			pos += 8

		case fieldTypeDecimal, fieldTypeNewDecimal, fieldTypeVarChar,
			fieldTypeBit, fieldTypeEnum, fieldTypeSet, fieldTypeTinyBLOB,
			fieldTypeMediumBLOB, fieldTypeLongBLOB, fieldTypeBLOB,
			fieldTypeVarString, fieldTypeString, fieldTypeGeometry, fieldTypeJSON,
			fieldTypeVector:
			var num uint64
			var isNull bool
			var n int
			num, isNull, n = readLengthEncodedInteger(data[pos:])
			pos += n
			if isNull {
				rows.rawCols[i] = nil
			} else {
				rows.rawCols[i] = data[pos : pos+int(num)]
				pos += int(num)
			}

		case fieldTypeDate, fieldTypeNewDate, fieldTypeTime,
			fieldTypeTimestamp, fieldTypeDateTime:
			var num uint64
			var isNull bool
			var n int
			num, isNull, n = readLengthEncodedInteger(data[pos:])
			pos += n
			if isNull {
				rows.rawCols[i] = nil
			} else {
				rows.rawCols[i] = data[pos : pos+int(num)]
				pos += int(num)
			}

		default:
			return fmt.Errorf("unknown field type %d", rows.rs.columns[i].fieldType)
		}
	}
	return nil
}

// ScanColumn implements driver.RowsColumnScanner.
// It converts the raw binary-protocol bytes for column i directly into dest,
// without any intermediate []driver.Value allocation.
func (rows *binaryRows) ScanColumn(scanCtx driver.ScanContext, i int, dest any) error {
	if i < 0 || i >= len(rows.rawCols) {
		return fmt.Errorf("mysql: column index %d out of range [0, %d)", i, len(rows.rawCols))
	}
	raw := rows.rawCols[i]
	if raw == nil {
		return sql.ConvertAssign(scanCtx, dest, nil)
	}

	col := rows.rs.columns[i]
	switch col.fieldType {
	case fieldTypeTiny:
		if col.flags&flagUnsigned != 0 {
			return sql.ConvertAssign(scanCtx, dest, int64(raw[0]))
		}
		return sql.ConvertAssign(scanCtx, dest, int64(int8(raw[0])))

	case fieldTypeShort, fieldTypeYear:
		if col.flags&flagUnsigned != 0 {
			return sql.ConvertAssign(scanCtx, dest, int64(binary.LittleEndian.Uint16(raw)))
		}
		return sql.ConvertAssign(scanCtx, dest, int64(int16(binary.LittleEndian.Uint16(raw))))

	case fieldTypeInt24, fieldTypeLong:
		if col.flags&flagUnsigned != 0 {
			return sql.ConvertAssign(scanCtx, dest, int64(binary.LittleEndian.Uint32(raw)))
		}
		return sql.ConvertAssign(scanCtx, dest, int64(int32(binary.LittleEndian.Uint32(raw))))

	case fieldTypeLongLong:
		if col.flags&flagUnsigned != 0 {
			val := binary.LittleEndian.Uint64(raw)
			if val > math.MaxInt64 {
				// Exceeds int64 range: represent as decimal string for compatibility.
				return sql.ConvertAssign(scanCtx, dest, uint64ToString(val))
			}
			return sql.ConvertAssign(scanCtx, dest, int64(val))
		}
		return sql.ConvertAssign(scanCtx, dest, int64(binary.LittleEndian.Uint64(raw)))

	case fieldTypeFloat:
		return sql.ConvertAssign(scanCtx, dest, math.Float32frombits(binary.LittleEndian.Uint32(raw)))

	case fieldTypeDouble:
		return sql.ConvertAssign(scanCtx, dest, math.Float64frombits(binary.LittleEndian.Uint64(raw)))

	case fieldTypeDecimal, fieldTypeNewDecimal, fieldTypeVarChar,
		fieldTypeBit, fieldTypeEnum, fieldTypeSet, fieldTypeTinyBLOB,
		fieldTypeMediumBLOB, fieldTypeLongBLOB, fieldTypeBLOB,
		fieldTypeVarString, fieldTypeString, fieldTypeGeometry, fieldTypeJSON,
		fieldTypeVector:
		// Fast paths for byte destinations.
		switch d := dest.(type) {
		case *[]byte:
			*d = append((*d)[:0], raw...)
			return nil
		case *sql.RawBytes:
			*d = raw // zero-copy; caller must not retain beyond the next Next/NextRow call
			return nil
		}
		return sql.ConvertAssign(scanCtx, dest, raw)

	case fieldTypeDate, fieldTypeNewDate, fieldTypeTimestamp, fieldTypeDateTime:
		if rows.mc.parseTime {
			val, err := parseBinaryDateTime(uint64(len(raw)), raw, rows.mc.cfg.Loc)
			if err != nil {
				return err
			}
			return sql.ConvertAssign(scanCtx, dest, val)
		}
		var dstlen uint8
		if col.fieldType == fieldTypeDate || col.fieldType == fieldTypeNewDate {
			dstlen = 10
		} else {
			switch decimals := col.decimals; decimals {
			case 0x00, 0x1f:
				dstlen = 19
			case 1, 2, 3, 4, 5, 6:
				dstlen = 19 + 1 + decimals
			default:
				return fmt.Errorf("protocol error, illegal decimals value %d", col.decimals)
			}
		}
		val, err := formatBinaryDateTime(raw, dstlen)
		if err != nil {
			return err
		}
		return sql.ConvertAssign(scanCtx, dest, val)

	case fieldTypeTime:
		var dstlen uint8
		switch decimals := col.decimals; decimals {
		case 0x00, 0x1f:
			dstlen = 8
		case 1, 2, 3, 4, 5, 6:
			dstlen = 8 + 1 + decimals
		default:
			return fmt.Errorf("protocol error, illegal decimals value %d", col.decimals)
		}
		val, err := formatBinaryTime(raw, dstlen)
		if err != nil {
			return err
		}
		return sql.ConvertAssign(scanCtx, dest, val)

	default:
		return fmt.Errorf("unknown field type %d", col.fieldType)
	}
}

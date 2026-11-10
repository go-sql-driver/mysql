// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2026 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"io"
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
	return data, nil
}

// readColumnBytes bounds-checks both the length prefix and the payload before
// slicing. A non-NULL empty value must remain distinct from nil.
func readColumnBytes(data []byte) (raw, rest []byte, err error) {
	if len(data) == 0 {
		return nil, nil, ErrMalformPkt
	}
	// Decode the length prefix once, after its bounds check.
	var n uint64
	switch data[0] {
	case 0xfb:
		return nil, data[1:], nil
	case 0xfc:
		if len(data) < 3 {
			return nil, nil, ErrMalformPkt
		}
		n, data = uint64(binary.LittleEndian.Uint16(data[1:])), data[3:]
	case 0xfd:
		if len(data) < 4 {
			return nil, nil, ErrMalformPkt
		}
		n, data = uint64(getUint24(data[1:])), data[4:]
	case 0xfe:
		if len(data) < 9 {
			return nil, nil, ErrMalformPkt
		}
		n, data = binary.LittleEndian.Uint64(data[1:]), data[9:]
	case 0xff:
		return nil, nil, ErrMalformPkt
	default:
		n, data = uint64(data[0]), data[1:]
	}
	if n > uint64(len(data)) {
		return nil, nil, ErrMalformPkt
	}
	return data[:int(n):int(n)], data[int(n):], nil
}

// emptyBytesValue is boxed once. A zero-capacity slice cannot be written
// through, so all empty values can share it.
var emptyBytesValue driver.Value = []byte{}

// bytesValue boxes a non-NULL column value without an allocation when empty.
func bytesValue(b []byte) driver.Value {
	if len(b) == 0 {
		return emptyBytesValue
	}
	return b
}

// binaryRowReader borrows the current packet. It is used by both Next and
// NextRow so NULL handling, field sizes, and bounds checks stay in one place.
type binaryRowReader struct {
	data     []byte
	nullMask []byte
}

func newBinaryRowReader(data []byte, columns int) (binaryRowReader, error) {
	// Two reserved bits precede the column bits in the NULL bitmap.
	pos := 1 + (columns+7+2)/8
	if len(data) < pos {
		return binaryRowReader{}, ErrMalformPkt
	}
	return binaryRowReader{data: data[pos:], nullMask: data[1:pos]}, nil
}

func (r *binaryRowReader) isNull(i int, fieldType fieldType) bool {
	return r.nullMask[(i+2)/8]&(1<<uint((i+2)%8)) != 0 || fieldType == fieldTypeNULL
}

var binaryFieldSizes = [256]uint8{
	fieldTypeTiny:  1,
	fieldTypeShort: 2, fieldTypeYear: 2,
	fieldTypeInt24: 4, fieldTypeLong: 4, fieldTypeFloat: 4,
	fieldTypeLongLong: 8, fieldTypeDouble: 8,
}

// Keep fixed-width columns inlineable. On false, readVariableColumn decodes a
// variable-width value or reports a truncated or unknown fixed-width column.
func (r *binaryRowReader) readFixedColumn(fieldType fieldType) ([]byte, bool) {
	size := int(binaryFieldSizes[fieldType])
	if size == 0 || len(r.data) < size {
		return nil, false
	}
	raw := r.data[:size:size]
	r.data = r.data[size:]
	return raw, true
}

// Variable-width types carry a length-encoded value.
var binaryVariableTypes = [256]bool{
	fieldTypeDecimal: true, fieldTypeNewDecimal: true, fieldTypeVarChar: true,
	fieldTypeBit: true, fieldTypeEnum: true, fieldTypeSet: true, fieldTypeTinyBLOB: true,
	fieldTypeMediumBLOB: true, fieldTypeLongBLOB: true, fieldTypeBLOB: true,
	fieldTypeVarString: true, fieldTypeString: true, fieldTypeGeometry: true, fieldTypeJSON: true,
	fieldTypeVector: true, fieldTypeDate: true, fieldTypeNewDate: true, fieldTypeTime: true,
	fieldTypeTimestamp: true, fieldTypeDateTime: true,
}

// readShortColumn reads a variable-width value with a one-byte length prefix.
// On false, readVariableColumn handles the value.
func (r *binaryRowReader) readShortColumn(fieldType fieldType) ([]byte, bool) {
	data := r.data
	if n := uint(len(data)); n == 0 || data[0] >= 0xfb || uint(data[0]) >= n || !binaryVariableTypes[fieldType] {
		return nil, false
	}
	n := uint(data[0]) + 1
	r.data = data[n:]
	return data[1:n:n], true
}

func (r *binaryRowReader) readVariableColumn(fieldType fieldType) ([]byte, error) {
	if binaryFieldSizes[fieldType] != 0 {
		return nil, ErrMalformPkt
	}
	if !binaryVariableTypes[fieldType] {
		return nil, fmt.Errorf("unknown field type %d", fieldType)
	}
	raw, rest, err := readColumnBytes(r.data)
	r.data = rest
	return raw, err
}

// The legacy Next path needs a boxed value, or an untyped nil on error.
// Keep the boxing code out of readRow's numeric hot loop.
//
//go:noinline
func formatBinaryColumnDateTimeValue(col mysqlField, raw []byte) (driver.Value, error) {
	value, err := formatBinaryColumnDateTime(col, raw)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func formatBinaryColumnDateTime(col mysqlField, raw []byte) ([]byte, error) {
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
			return nil, fmt.Errorf("protocol error, illegal decimals value %d", col.decimals)
		}
	}
	if col.fieldType == fieldTypeTime {
		return formatBinaryTime(raw, length)
	}
	return formatBinaryDateTime(raw, length)
}

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

func (r *binaryRowReader) readVariableColumn(fieldType fieldType) ([]byte, error) {
	if binaryFieldSizes[fieldType] != 0 {
		return nil, ErrMalformPkt
	}
	switch fieldType {
	case fieldTypeDecimal, fieldTypeNewDecimal, fieldTypeVarChar,
		fieldTypeBit, fieldTypeEnum, fieldTypeSet, fieldTypeTinyBLOB,
		fieldTypeMediumBLOB, fieldTypeLongBLOB, fieldTypeBLOB,
		fieldTypeVarString, fieldTypeString, fieldTypeGeometry, fieldTypeJSON,
		fieldTypeVector, fieldTypeDate, fieldTypeNewDate, fieldTypeTime,
		fieldTypeTimestamp, fieldTypeDateTime:
		raw, rest, err := readColumnBytes(r.data)
		r.data = rest
		return raw, err
	default:
		return nil, fmt.Errorf("unknown field type %d", fieldType)
	}
}

func formatBinaryColumnDateTime(col mysqlField, raw []byte) (driver.Value, error) {
	length := uint8(19)
	switch col.fieldType {
	case fieldTypeDate:
		length = 10
	case fieldTypeTime:
		length = 8
	}
	// Preserve Next's DATETIME formatting for fieldTypeNewDate.
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

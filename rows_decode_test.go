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
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"testing"
	"time"
)

func rowsTestPacket(bp bool, columns []mysqlField, payload []byte, parseTime bool) (driver.Rows, *mockConn, *mysqlConn) {
	conn, mc := newRWMockConn(0)
	mc.parseTime = parseTime
	conn.data = append([]byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16), 0}, payload...)
	rows := mysqlRows{mc: mc, rs: resultSet{columns: columns}}
	if bp {
		return &binaryRows{rows}, conn, mc
	}
	return &textRows{rows}, conn, mc
}

// These expectations are independent of either scanning path. In particular,
// preserve the different driver.Value types used for unsigned BIGINT columns.
func TestRowsDecodedValues(t *testing.T) {
	date := time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC)
	columns := []struct {
		field        mysqlField
		text, binary []byte
		textValue    driver.Value
		binaryValue  driver.Value
	}{
		{mysqlField{fieldType: fieldTypeTiny, length: 1}, []byte("-2"), []byte{254}, true, true},
		{mysqlField{fieldType: fieldTypeTiny, length: 2}, []byte("-2"), []byte{254}, int64(-2), int64(-2)},
		{mysqlField{fieldType: fieldTypeTiny, flags: flagUnsigned, length: 1}, []byte("254"), []byte{254}, int64(254), int64(254)},
		{mysqlField{fieldType: fieldTypeString}, nil, nil, nil, nil},
		{mysqlField{fieldType: fieldTypeShort}, []byte("-32768"), []byte{0, 128}, int64(-32768), int64(-32768)},
		{mysqlField{fieldType: fieldTypeYear}, []byte("2026"), []byte{0xea, 7}, int64(2026), int64(2026)},
		{mysqlField{fieldType: fieldTypeLong, flags: flagUnsigned}, []byte("4294967295"), []byte{255, 255, 255, 255}, int64(4294967295), int64(4294967295)},
		{mysqlField{fieldType: fieldTypeInt24}, []byte("-42"), binary.LittleEndian.AppendUint32(nil, 0xffffffd6), int64(-42), int64(-42)},
		{mysqlField{fieldType: fieldTypeLongLong}, []byte("-9223372036854775808"), binary.LittleEndian.AppendUint64(nil, 1<<63), int64(math.MinInt64), int64(math.MinInt64)},
		{mysqlField{fieldType: fieldTypeLongLong, flags: flagUnsigned}, []byte("42"), binary.LittleEndian.AppendUint64(nil, 42), uint64(42), int64(42)},
		{mysqlField{fieldType: fieldTypeLongLong, flags: flagUnsigned}, []byte("18446744073709551615"), binary.LittleEndian.AppendUint64(nil, math.MaxUint64), uint64(math.MaxUint64), []byte("18446744073709551615")},
		{mysqlField{fieldType: fieldTypeFloat}, []byte("1.25"), binary.LittleEndian.AppendUint32(nil, math.Float32bits(1.25)), float32(1.25), float32(1.25)},
		{mysqlField{fieldType: fieldTypeDouble}, []byte("-2.5"), binary.LittleEndian.AppendUint64(nil, math.Float64bits(-2.5)), float64(-2.5), float64(-2.5)},
		{mysqlField{fieldType: fieldTypeNewDecimal}, []byte("123.4500"), []byte("123.4500"), []byte("123.4500"), []byte("123.4500")},
		{mysqlField{fieldType: fieldTypeString}, []byte{}, []byte{}, []byte{}, []byte{}},
		{mysqlField{fieldType: fieldTypeBLOB}, []byte{0, 255}, []byte{0, 255}, []byte{0, 255}, []byte{0, 255}},
		{mysqlField{fieldType: fieldTypeNULL}, nil, nil, nil, nil},
		{mysqlField{fieldType: fieldTypeDate}, []byte("2026-09-30"), []byte{0xea, 7, 9, 30}, []byte("2026-09-30"), []byte("2026-09-30")},
		{mysqlField{fieldType: fieldTypeNewDate}, []byte("2026-09-30"), []byte{0xea, 7, 9, 30}, []byte("2026-09-30"), []byte("2026-09-30")},
		{mysqlField{fieldType: fieldTypeTime}, []byte("-49:02:03"), []byte{1, 2, 0, 0, 0, 1, 2, 3}, []byte("-49:02:03"), []byte("-49:02:03")},
	}
	for _, bp := range []bool{false, true} {
		for _, parseTime := range []bool{false, true} {
			t.Run(fmt.Sprintf("binary=%v/parseTime=%v", bp, parseTime), func(t *testing.T) {
				fields := make([]mysqlField, len(columns))
				want := make([]driver.Value, len(columns))
				var payload []byte
				if bp {
					payload = make([]byte, 1+(len(columns)+7+2)/8)
				}
				for i, col := range columns {
					fields[i] = col.field
					want[i] = col.textValue
					if bp {
						want[i] = col.binaryValue
						if col.binary == nil {
							payload[1+(i+2)/8] |= 1 << uint((i+2)%8)
							continue
						}
						switch col.field.fieldType {
						case fieldTypeTiny, fieldTypeShort, fieldTypeYear, fieldTypeInt24, fieldTypeLong, fieldTypeLongLong, fieldTypeFloat, fieldTypeDouble:
							payload = append(payload, col.binary...)
						default:
							payload = appendLengthEncodedString(payload, string(col.binary))
						}
					} else if col.text == nil {
						payload = append(payload, 0xfb)
					} else {
						payload = appendLengthEncodedString(payload, string(col.text))
					}
					if parseTime && (col.field.fieldType == fieldTypeDate || col.field.fieldType == fieldTypeNewDate) {
						want[i] = date
					}
				}
				rows, _, _ := rowsTestPacket(bp, fields, payload, parseTime)
				got := make([]driver.Value, len(columns))
				if err := rows.Next(got); err != nil {
					t.Fatal(err)
				}
				for i := range got {
					if !reflect.DeepEqual(got[i], want[i]) {
						t.Errorf("column %d: got %T(%v), want %T(%v)", i, got[i], got[i], want[i], want[i])
					}
				}
			})
		}
	}
}

func TestRowsMalformedPackets(t *testing.T) {
	for _, bp := range []bool{false, true} {
		for _, payload := range [][]byte{{0xfc}, {0xfd, 1}, {5, 'a'}, {1, 'a', 0}} {
			if bp {
				payload = append([]byte{0, 0}, payload...)
			}
			rows, _, _ := rowsTestPacket(bp, []mysqlField{{fieldType: fieldTypeString}}, payload, false)
			if err := rows.Next(make([]driver.Value, 1)); !errors.Is(err, ErrMalformPkt) {
				t.Fatalf("binary=%v packet=%x: %v", bp, payload, err)
			}
		}
	}
	for _, payload := range [][]byte{{0}, {0, 0, 1}} {
		rows, _, _ := rowsTestPacket(true, []mysqlField{{fieldType: fieldTypeLongLong}}, payload, false)
		if err := rows.Next(make([]driver.Value, 1)); !errors.Is(err, ErrMalformPkt) {
			t.Fatalf("truncated binary packet %x: %v", payload, err)
		}
	}
}

func TestRowsRepeatedEOF(t *testing.T) {
	for _, bp := range []bool{false, true} {
		for _, deprecateEOF := range []bool{false, true} {
			payload := []byte{iEOF, 0, 0, byte(statusMoreResultsExists), 0}
			if deprecateEOF {
				payload = append(payload, 0, 0)
			}
			rows, conn, mc := rowsTestPacket(bp, []mysqlField{{fieldType: fieldTypeString}}, payload, false)
			if deprecateEOF {
				mc.capabilities |= clientDeprecateEOF
			}
			values := make([]driver.Value, 1)
			if err := rows.Next(values); err != io.EOF {
				t.Fatal(err)
			}
			reads := conn.reads
			if err := rows.Next(values); err != io.EOF {
				t.Fatal(err)
			}
			if conn.reads != reads || !rows.(driver.RowsNextResultSet).HasNextResultSet() {
				t.Fatal("read beyond EOF or lost next result set status")
			}
		}
	}
}

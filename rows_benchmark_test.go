// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2026 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"strconv"
	"testing"
)

// Exercise the legacy driver.Rows path independently of database/sql's choice
// of scanning interface, including allocation costs for wide and large rows.
func BenchmarkRowsNext(b *testing.B) {
	for _, bp := range []bool{false, true} {
		for _, columns := range []int{1, 32} {
			for _, value := range []string{"42", "123456789", "blob"} {
				b.Run(fmt.Sprintf("binary=%v/columns=%d/value=%s", bp, columns, value), func(b *testing.B) {
					var payload []byte
					if bp {
						payload = make([]byte, 1+(columns+7+2)/8)
					}
					fields := make([]mysqlField, columns)
					for i := range fields {
						fields[i].fieldType = fieldTypeLongLong
						if value == "blob" {
							fields[i].fieldType = fieldTypeBLOB
							payload = appendLengthEncodedString(payload, string(bytes.Repeat([]byte{'x'}, 4096)))
						} else if bp {
							n, _ := strconv.ParseUint(value, 10, 64)
							payload = binary.LittleEndian.AppendUint64(payload, n)
						} else {
							payload = appendLengthEncodedString(payload, value)
						}
					}
					packet := append([]byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16), 0}, payload...)
					conn, mc := newRWMockConn(0)
					base := mysqlRows{mc: mc, rs: resultSet{columns: fields}}
					var rows driver.Rows = &textRows{base}
					if bp {
						rows = &binaryRows{base}
					}
					values := make([]driver.Value, columns)
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						conn.data = packet
						mc.sequence = 0
						if err := rows.Next(values); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}

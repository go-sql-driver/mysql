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
	"io"
	"math"
	"reflect"
	"testing"
	"time"
)

func scannerTestRows(binaryProtocol bool, columns []mysqlField, payload []byte, parseTime bool) (driver.RowsColumnScanner, *mockConn, *mysqlConn) {
	rows, conn, mc := rowsTestPacket(binaryProtocol, columns, payload, parseTime)
	return rows.(driver.RowsColumnScanner), conn, mc
}

// Retain the exact source type passed to a custom Scanner.
type columnScannerCapture struct{ value any }

func (s *columnScannerCapture) Scan(value any) error { s.value = value; return nil }

type columnScannerInt int64

func TestRowsColumnScannerConversions(t *testing.T) {
	date := []byte{0xea, 7, 9, 30, 12, 34, 56, 0x40, 0xe2, 1, 0}
	tests := []struct {
		name         string
		column       mysqlField
		text, binary []byte
		parseTime    bool
	}{
		{name: "null", column: mysqlField{fieldType: fieldTypeString}},
		{name: "empty", column: mysqlField{fieldType: fieldTypeString}, text: []byte{}, binary: []byte{}},
		{name: "string", column: mysqlField{fieldType: fieldTypeString}, text: []byte("hello"), binary: []byte("hello")},
		{name: "blob", column: mysqlField{fieldType: fieldTypeBLOB}, text: []byte{0, 255}, binary: []byte{0, 255}},
		{name: "bool", column: mysqlField{fieldType: fieldTypeTiny, length: 1}, text: []byte("-2"), binary: []byte{254}},
		{name: "tiny", column: mysqlField{fieldType: fieldTypeTiny, length: 2}, text: []byte("-2"), binary: []byte{254}},
		{name: "unsigned tiny", column: mysqlField{fieldType: fieldTypeTiny, length: 1, flags: flagUnsigned}, text: []byte("254"), binary: []byte{254}},
		{name: "zerofill tiny", column: mysqlField{fieldType: fieldTypeTiny, length: 1, flags: flagZeroFill}, text: []byte("2"), binary: []byte{2}},
		{name: "short", column: mysqlField{fieldType: fieldTypeShort}, text: []byte("-1234"), binary: binary.LittleEndian.AppendUint16(nil, 65536-1234)},
		{name: "long", column: mysqlField{fieldType: fieldTypeLong}, text: []byte("1234567"), binary: binary.LittleEndian.AppendUint32(nil, 1234567)},
		{name: "longlong", column: mysqlField{fieldType: fieldTypeLongLong}, text: []byte("-9223372036854775808"), binary: binary.LittleEndian.AppendUint64(nil, 1<<63)},
		{name: "uint64", column: mysqlField{fieldType: fieldTypeLongLong, flags: flagUnsigned}, text: []byte("18446744073709551615"), binary: binary.LittleEndian.AppendUint64(nil, math.MaxUint64)},
		{name: "float", column: mysqlField{fieldType: fieldTypeFloat}, text: []byte("1.23456789"), binary: binary.LittleEndian.AppendUint32(nil, math.Float32bits(1.23456789))},
		{name: "double", column: mysqlField{fieldType: fieldTypeDouble}, text: []byte("1.23456789"), binary: binary.LittleEndian.AppendUint64(nil, math.Float64bits(1.23456789))},
		{name: "decimal", column: mysqlField{fieldType: fieldTypeNewDecimal}, text: []byte("123.4500"), binary: []byte("123.4500")},
		{name: "datetime", column: mysqlField{fieldType: fieldTypeDateTime, decimals: 6}, text: []byte("2026-09-30 12:34:56.123456"), binary: date},
		{name: "parsed datetime", column: mysqlField{fieldType: fieldTypeDateTime, decimals: 6}, text: []byte("2026-09-30 12:34:56.123456"), binary: date, parseTime: true},
		{name: "zero date", column: mysqlField{fieldType: fieldTypeDate}, text: []byte("0000-00-00"), binary: []byte{}},
		{name: "parsed zero date", column: mysqlField{fieldType: fieldTypeDate}, text: []byte("0000-00-00"), binary: []byte{}, parseTime: true},
		{name: "time", column: mysqlField{fieldType: fieldTypeTime}, text: []byte("-49:02:03"), binary: []byte{1, 2, 0, 0, 0, 1, 2, 3}},
	}
	destinations := []func() any{
		func() any { return new(any) }, func() any { return new(string) }, func() any { return new([]byte) },
		func() any { return new(int) }, func() any { return new(int8) }, func() any { return new(int64) },
		func() any { return new(uint64) }, func() any { return new(float32) }, func() any { return new(float64) },
		func() any { return new(bool) }, func() any { return new(time.Time) }, func() any { return new(sql.NullString) },
		func() any { return new(sql.NullInt64) }, func() any { return new(columnScannerCapture) },
		func() any { return new(columnScannerInt) }, func() any { return new(*int64) },
	}
	for _, tt := range tests {
		for _, bp := range []bool{false, true} {
			protocol := "text/"
			if bp {
				protocol = "binary/"
			}
			t.Run(protocol+tt.name, func(t *testing.T) {
				var payload []byte
				if bp {
					payload = []byte{0, 0}
					if tt.binary == nil {
						payload[1] = 4
					} else {
						switch tt.column.fieldType {
						case fieldTypeTiny, fieldTypeShort, fieldTypeLong, fieldTypeLongLong, fieldTypeFloat, fieldTypeDouble:
							payload = append(payload, tt.binary...)
						default:
							payload = appendLengthEncodedInteger(payload, uint64(len(tt.binary)))
							payload = append(payload, tt.binary...)
						}
					}
				} else if tt.text == nil {
					payload = []byte{0xfb}
				} else {
					payload = appendLengthEncodedString(nil, string(tt.text))
				}
				columns := []mysqlField{tt.column}
				old, _, _ := scannerTestRows(bp, columns, payload, tt.parseTime)
				want := make([]driver.Value, 1)
				if err := old.Next(want); err != nil {
					t.Fatal(err)
				}
				rows, _, _ := scannerTestRows(bp, columns, payload, tt.parseTime)
				if err := rows.NextRow(); err != nil {
					t.Fatal(err)
				}
				for _, makeDest := range destinations {
					gotDest, wantDest := makeDest(), makeDest()
					wantErr := sql.ConvertAssign(driver.ScanContext{}, wantDest, want[0])
					gotErr := rows.ScanColumn(driver.ScanContext{}, 0, gotDest)
					if (wantErr == nil) != (gotErr == nil) {
						t.Errorf("%T: error %v, want %v", gotDest, gotErr, wantErr)
						continue
					}
					if gotErr == nil && !reflect.DeepEqual(gotDest, wantDest) {
						t.Errorf("%T: got %#v, want %#v", gotDest, reflect.ValueOf(gotDest).Elem().Interface(), reflect.ValueOf(wantDest).Elem().Interface())
					}
				}
			})
		}
	}
}

func TestRowsColumnScannerBytesOwnership(t *testing.T) {
	for _, bp := range []bool{false, true} {
		payload := []byte{3, 'a', 'b', 'c', 0}
		if bp {
			payload = append([]byte{0, 0}, payload...)
		}
		rows, _, _ := scannerTestRows(bp, []mysqlField{{fieldType: fieldTypeString}, {fieldType: fieldTypeString}}, payload, false)
		if err := rows.NextRow(); err != nil {
			t.Fatal(err)
		}
		var got []byte
		if err := rows.ScanColumn(driver.ScanContext{}, 0, &got); err != nil {
			t.Fatal(err)
		}
		first := got
		if err := rows.ScanColumn(driver.ScanContext{}, 0, &got); err != nil {
			t.Fatal(err)
		}
		got[0] = 'x'
		if string(first) != "abc" {
			t.Fatal("Scan reused the previous destination's backing array")
		}
		var raw sql.RawBytes
		if err := rows.ScanColumn(driver.ScanContext{}, 0, &raw); err != nil {
			t.Fatal(err)
		}
		if string(raw) != "abc" {
			t.Fatal("copy modified the packet")
		}
		if err := rows.ScanColumn(driver.ScanContext{}, 1, &got); err != nil {
			t.Fatal(err)
		}
		if got == nil || len(got) != 0 {
			t.Fatalf("empty column = %#v", got)
		}
		for _, dest := range []any{(*[]byte)(nil), (*sql.RawBytes)(nil), (*string)(nil)} {
			if err := rows.ScanColumn(driver.ScanContext{}, 0, dest); err == nil {
				t.Fatalf("accepted %T(nil)", dest)
			}
		}
	}
}

func TestRowsColumnScannerNilDestinations(t *testing.T) {
	ctx := driver.ScanContext{}
	nilPtrErr := sql.ConvertAssign(ctx, (*int64)(nil), int64(42))
	if nilPtrErr == nil {
		t.Fatal("ConvertAssign accepted a nil destination")
	}

	tests := []struct {
		name   string
		column mysqlField
		text   []byte
		binary []byte
		value  driver.Value
		dest   any
	}{
		{"int", mysqlField{fieldType: fieldTypeLong}, []byte("42"), binary.LittleEndian.AppendUint32(nil, 42), int64(42), (*int)(nil)},
		{"int64", mysqlField{fieldType: fieldTypeLongLong}, []byte("42"), binary.LittleEndian.AppendUint64(nil, 42), int64(42), (*int64)(nil)},
		{"uint64", mysqlField{fieldType: fieldTypeLongLong, flags: flagUnsigned}, []byte("42"), binary.LittleEndian.AppendUint64(nil, 42), uint64(42), (*uint64)(nil)},
		{"float32", mysqlField{fieldType: fieldTypeFloat}, []byte("1.25"), binary.LittleEndian.AppendUint32(nil, math.Float32bits(1.25)), float32(1.25), (*float32)(nil)},
		{"float64", mysqlField{fieldType: fieldTypeDouble}, []byte("1.25"), binary.LittleEndian.AppendUint64(nil, math.Float64bits(1.25)), float64(1.25), (*float64)(nil)},
		{"bool", mysqlField{fieldType: fieldTypeTiny, length: 1}, []byte("1"), []byte{1}, true, (*bool)(nil)},
		{"time", mysqlField{fieldType: fieldTypeDate}, []byte("2026-09-30"), []byte{0xea, 7, 9, 30}, time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC), (*time.Time)(nil)},
	}
	for _, tt := range tests {
		for _, bp := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/binary=%v", tt.name, bp), func(t *testing.T) {
				_, mc := newRWMockConn(0)
				mc.parseTime = true
				raw := tt.text
				if bp {
					raw = tt.binary
				}
				base := mysqlRows{mc: mc, rs: resultSet{columns: []mysqlField{tt.column}}, rawCols: [][]byte{raw}}
				var rows driver.RowsColumnScanner = &textRows{base}
				if bp {
					rows = &binaryRows{base}
				}
				want := nilPtrErr.Error()
				switch tt.value.(type) {
				case bool, time.Time:
					// ConvertAssign can panic for these typed-nil destinations.
					// Reject them with an error instead of reproducing the panic.
				default:
					err := sql.ConvertAssign(ctx, tt.dest, tt.value)
					if err == nil {
						t.Fatal("ConvertAssign accepted a nil destination")
					}
					want = err.Error()
				}
				if err := rows.ScanColumn(ctx, 0, tt.dest); err == nil || err.Error() != want {
					t.Fatalf("ScanColumn error = %v, want %q", err, want)
				}
			})
		}
	}
}

func TestRowsColumnScannerInvalidation(t *testing.T) {
	for _, bp := range []bool{false, true} {
		for _, closeRows := range []bool{false, true} {
			for _, connError := range []bool{false, true} {
				t.Run(fmt.Sprintf("binary=%v/close=%v/error=%v", bp, closeRows, connError), func(t *testing.T) {
					payload := appendLengthEncodedString(nil, "2026-09-30")
					if bp {
						payload = []byte{0, 0, 4, 0xea, 7, 9, 30}
					}
					rows, conn, mc := scannerTestRows(bp, []mysqlField{{fieldType: fieldTypeDate}}, payload, true)
					conn.data = append(conn.data, 5, 0, 0, 1, iEOF, 0, 0, 0, 0)
					if err := rows.NextRow(); err != nil {
						t.Fatal(err)
					}
					var date time.Time
					if err := rows.ScanColumn(driver.ScanContext{}, 0, &date); err != nil {
						t.Fatal(err)
					}
					if connError {
						mc.closed.Store(true)
					}
					var err, want error
					if closeRows {
						err = rows.Close()
					} else {
						err = rows.(driver.RowsNextResultSet).NextResultSet()
						want = io.EOF
					}
					if connError {
						want = ErrInvalidConn
					}
					if !errors.Is(err, want) {
						t.Fatalf("row transition error = %v, want %v", err, want)
					}
					if err := rows.ScanColumn(driver.ScanContext{}, 0, &date); err == nil {
						t.Fatal("ScanColumn accepted the previous row after a row transition")
					}
				})
			}
		}
	}
}

func TestRowsColumnScannerNewDate(t *testing.T) {
	for _, decimals := range []uint8{0, 6, 0x1f} {
		for _, zero := range []bool{false, true} {
			t.Run(fmt.Sprintf("decimals=%d/zero=%v", decimals, zero), func(t *testing.T) {
				payload := []byte{0, 0, 4, 0xea, 7, 9, 30}
				want := "2026-09-30"
				if zero {
					payload = []byte{0, 0, 0}
					want = "0000-00-00"
				}
				columns := []mysqlField{{fieldType: fieldTypeNewDate, decimals: decimals}}
				old, _, _ := scannerTestRows(true, columns, payload, false)
				values := make([]driver.Value, 1)
				if err := old.Next(values); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(values[0], []byte(want)) {
					t.Fatalf("Next value = %#v, want %q", values[0], want)
				}
				rows, _, _ := scannerTestRows(true, columns, payload, false)
				if err := rows.NextRow(); err != nil {
					t.Fatal(err)
				}
				var got any
				if err := rows.ScanColumn(driver.ScanContext{}, 0, &got); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got, []byte(want)) {
					t.Fatalf("ScanColumn value = %#v, want %q", got, want)
				}
			})
		}
	}
}

func TestRowsColumnScannerPackets(t *testing.T) {
	for _, bp := range []bool{false, true} {
		for _, deprecateEOF := range []bool{false, true} {
			payload := []byte{iEOF, 0, 0, byte(statusMoreResultsExists), 0}
			if deprecateEOF {
				payload = append(payload, 0, 0)
			}
			rows, conn, mc := scannerTestRows(bp, []mysqlField{{fieldType: fieldTypeString}}, payload, false)
			if deprecateEOF {
				mc.capabilities |= clientDeprecateEOF
			}
			if err := rows.NextRow(); err != io.EOF {
				t.Fatalf("EOF: %v", err)
			}
			reads := conn.reads
			if err := rows.NextRow(); err != io.EOF {
				t.Fatalf("second EOF: %v", err)
			}
			if conn.reads != reads {
				t.Fatal("read past EOF into the next result set")
			}
			if mc.status&statusMoreResultsExists == 0 {
				t.Fatal("lost next result set status")
			}
			if err := rows.ScanColumn(driver.ScanContext{}, 0, new(any)); err == nil {
				t.Fatal("scanned after EOF")
			}
		}
	}
	for _, bp := range []bool{false, true} {
		for _, payload := range [][]byte{{0xfc}, {0xfd, 1}, {0xfe, 1}, {5, 'a'}, {0xfc, 255, 255}} {
			if bp {
				payload = append([]byte{0, 0}, payload...)
			}
			rows, _, _ := scannerTestRows(bp, []mysqlField{{fieldType: fieldTypeString}}, payload, false)
			if err := rows.NextRow(); !errors.Is(err, ErrMalformPkt) {
				t.Fatalf("binary=%v packet %x: %v", bp, payload, err)
			}
		}
	}
	rows, _, _ := scannerTestRows(true, []mysqlField{{fieldType: fieldTypeLongLong}}, []byte{0, 0, 1}, false)
	if err := rows.NextRow(); !errors.Is(err, ErrMalformPkt) {
		t.Fatalf("truncated integer: %v", err)
	}
}

func TestRowsColumnScannerAllocations(t *testing.T) {
	cases := []struct {
		name      string
		col       mysqlField
		raw       []byte
		dest      any
		binary    bool
		parseTime bool
	}{
		{name: "text int64", col: mysqlField{fieldType: fieldTypeLongLong}, raw: []byte("123456789"), dest: new(int64)},
		{name: "text int", col: mysqlField{fieldType: fieldTypeLong}, raw: []byte("123456789"), dest: new(int)},
		{name: "text double", col: mysqlField{fieldType: fieldTypeDouble}, raw: []byte("1.2345"), dest: new(float64)},
		{name: "text time", col: mysqlField{fieldType: fieldTypeDateTime}, raw: []byte("2026-09-30 12:34:56"), dest: new(time.Time), parseTime: true},
		{name: "text raw", col: mysqlField{fieldType: fieldTypeString}, raw: []byte("hello"), dest: new(sql.RawBytes)},
		{name: "binary int", col: mysqlField{fieldType: fieldTypeLong}, raw: binary.LittleEndian.AppendUint32(nil, 123456789), dest: new(int), binary: true},
		{name: "binary uint64", col: mysqlField{fieldType: fieldTypeLongLong, flags: flagUnsigned}, raw: binary.LittleEndian.AppendUint64(nil, math.MaxUint64), dest: new(uint64), binary: true},
		{name: "binary time", col: mysqlField{fieldType: fieldTypeDateTime}, raw: []byte{0xea, 7, 9, 30, 12, 34, 56}, dest: new(time.Time), binary: true, parseTime: true},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			_, mc := newRWMockConn(0)
			mc.parseTime = tt.parseTime
			base := mysqlRows{mc: mc, rs: resultSet{columns: []mysqlField{tt.col}}, rawCols: [][]byte{tt.raw}}
			var rows driver.RowsColumnScanner = &textRows{base}
			if tt.binary {
				rows = &binaryRows{base}
			}
			allocs := testing.AllocsPerRun(100, func() {
				if err := rows.ScanColumn(driver.ScanContext{}, 0, tt.dest); err != nil {
					panic(err)
				}
			})
			if allocs != 0 {
				t.Fatalf("ScanColumn allocations = %v, want 0", allocs)
			}
		})
	}
	// Warmed-up row decoding reuses the per-column slice and packet buffer.
	for _, bp := range []bool{false, true} {
		payload := []byte{3, 'a', 'b', 'c'}
		if bp {
			payload = append([]byte{0, 0}, payload...)
		}
		rows, conn, mc := scannerTestRows(bp, []mysqlField{{fieldType: fieldTypeString}}, payload, false)
		packet := bytes.Clone(conn.data)
		allocs := testing.AllocsPerRun(100, func() {
			conn.data = packet
			mc.sequence = 0
			if err := rows.NextRow(); err != nil {
				panic(err)
			}
		})
		if allocs != 0 {
			t.Fatalf("binary=%v: NextRow allocations = %v, want 0", bp, allocs)
		}
	}
}

func TestRowsColumnScannerRawBytes(t *testing.T) {
	// Numeric and parsed-time RawBytes conversions require the ScanContext supplied
	// by database/sql. Exercise both protocol paths and repeated Scan calls.
	for _, prepared := range []bool{false, true} {
		name := "text"
		if prepared {
			name = "binary"
		}
		t.Run(name, func(t *testing.T) {
			runTestsParallel(t, dsn+"&parseTime=true", func(dbt *DBTest, _ string) {
				query := "SELECT CAST(123456789 AS SIGNED), CAST('2026-09-30 12:34:56' AS DATETIME), 'abc', ''"
				var rows *sql.Rows
				if prepared {
					stmt, err := dbt.db.Prepare(query)
					if err != nil {
						dbt.Fatal(err)
					}
					defer stmt.Close()
					rows, err = stmt.Query()
					if err != nil {
						dbt.Fatal(err)
					}
				} else {
					rows = dbt.mustQuery(query)
				}
				defer rows.Close()
				if !rows.Next() {
					dbt.Fatalf("Next: %v", rows.Err())
				}
				var copies [4][]byte
				if err := rows.Scan(&copies[0], &copies[1], &copies[2], &copies[3]); err != nil {
					dbt.Fatal(err)
				}
				var number, date, str, empty sql.RawBytes
				if err := rows.Scan(&number, &date, &str, &empty); err != nil {
					dbt.Fatal(err)
				}
				if string(number) != "123456789" || string(date) != "2026-09-30T12:34:56Z" || string(str) != "abc" || empty == nil || len(empty) != 0 {
					dbt.Fatalf("prepared=%v: got %q, %q, %q, %#v", prepared, number, date, str, empty)
				}
				for i, raw := range []sql.RawBytes{number, date, str, empty} {
					if !bytes.Equal(copies[i], raw) {
						dbt.Fatalf("column %d: copy %q, raw %q", i, copies[i], raw)
					}
				}
				if rows.Next() || rows.Err() != nil {
					dbt.Fatalf("EOF: %v", rows.Err())
				}
			})
		})
	}
}

// Include packet decoding, every column assignment, and the per-result column
// slice. Reuse the connection and destinations, as database/sql callers can do.
func BenchmarkRowsScan(b *testing.B) {
	date := []byte{0xea, 7, 9, 30, 12, 34, 56}
	cases := []struct {
		name      string
		field     fieldType
		text      string
		binary    []byte
		parseTime bool
		dest      func() any
	}{
		{"int64", fieldTypeLongLong, "123456789", binary.LittleEndian.AppendUint64(nil, 123456789), false, func() any { return new(int64) }},
		{"null-int64", fieldTypeLongLong, "123456789", binary.LittleEndian.AppendUint64(nil, 123456789), false, func() any { return new(sql.NullInt64) }},
		{"string", fieldTypeString, "hello world", []byte("hello world"), false, func() any { return new(string) }},
		{"bytes", fieldTypeString, "hello world", []byte("hello world"), false, func() any { return new([]byte) }},
		{"any", fieldTypeString, "hello world", []byte("hello world"), false, func() any { return new(any) }},
		{"date-string", fieldTypeDate, "2026-09-30", date[:4], false, func() any { return new(string) }},
		{"date-bytes", fieldTypeDate, "2026-09-30", date[:4], false, func() any { return new([]byte) }},
		{"date-raw", fieldTypeDate, "2026-09-30", date[:4], false, func() any { return new(sql.RawBytes) }},
		{"date-any", fieldTypeDate, "2026-09-30", date[:4], false, func() any { return new(any) }},
		{"datetime-string", fieldTypeDateTime, "2026-09-30 12:34:56", date, false, func() any { return new(string) }},
		{"datetime-time", fieldTypeDateTime, "2026-09-30 12:34:56", date, true, func() any { return new(time.Time) }},
		{"datetime-nulltime", fieldTypeDateTime, "2026-09-30 12:34:56", date, true, func() any { return new(sql.NullTime) }},
		{"time-string", fieldTypeTime, "12:34:56", []byte{0, 0, 0, 0, 0, 12, 34, 56}, false, func() any { return new(string) }},
	}
	for _, tt := range cases {
		for _, bp := range []bool{false, true} {
			for _, count := range []int{1, 100} {
				for _, direct := range []bool{false, true} {
					b.Run(fmt.Sprintf("%s/binary=%v/rows=%d/direct=%v", tt.name, bp, count, direct), func(b *testing.B) {
						payload := appendLengthEncodedString(nil, tt.text)
						if bp {
							payload = []byte{0, 0}
							if tt.field != fieldTypeLongLong {
								payload = appendLengthEncodedInteger(payload, uint64(len(tt.binary)))
							}
							payload = append(payload, tt.binary...)
						}
						rows, conn, mc := scannerTestRows(bp, []mysqlField{{fieldType: tt.field}}, payload, tt.parseTime)
						packet := conn.data
						var base *mysqlRows
						if bp {
							base = &rows.(*binaryRows).mysqlRows
						} else {
							base = &rows.(*textRows).mysqlRows
						}
						dest := tt.dest()
						b.ReportAllocs()
						b.ResetTimer()
						for range b.N {
							// Both APIs allocate their column slice once per result.
							var values []driver.Value
							if direct {
								base.rawCols = nil
							} else {
								values = make([]driver.Value, 1)
							}
							for range count {
								conn.data = packet
								mc.sequence = 0
								if direct {
									if err := rows.NextRow(); err != nil {
										b.Fatal(err)
									}
									if err := rows.ScanColumn(driver.ScanContext{}, 0, dest); err != nil {
										b.Fatal(err)
									}
								} else {
									if err := rows.Next(values); err != nil {
										b.Fatal(err)
									}
									if err := sql.ConvertAssign(driver.ScanContext{}, dest, values[0]); err != nil {
										b.Fatal(err)
									}
								}
							}
						}
					})
				}
			}
		}
	}
}

func TestScanColumnBytesAnyOwnership(t *testing.T) {
	for _, raw := range [][]byte{nil, {}, []byte("abc")} {
		var first, second any
		if err := scanColumnBytes(driver.ScanContext{}, &first, raw); err != nil {
			t.Fatal(err)
		}
		if err := scanColumnBytes(driver.ScanContext{}, &second, raw); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(first, raw) || !reflect.DeepEqual(second, raw) {
			t.Fatalf("copies = %#v, %#v; want %#v", first, second, raw)
		}
		if len(raw) > 0 {
			second.([]byte)[0] = 'x'
			if !bytes.Equal(first.([]byte), raw) || string(raw) != "abc" {
				t.Fatal("any destination aliases source or previous destination")
			}
		}
	}
	if err := scanColumnBytes(driver.ScanContext{}, (*any)(nil), []byte("abc")); err == nil {
		t.Fatal("accepted nil destination")
	}
}

func TestRowsColumnScannerZeroDateTimeOwnership(t *testing.T) {
	for _, field := range []fieldType{fieldTypeDate, fieldTypeDateTime, fieldTypeTime} {
		rows, _, _ := scannerTestRows(true, []mysqlField{{fieldType: field}}, []byte{0, 0, 0}, false)
		if err := rows.NextRow(); err != nil {
			t.Fatal(err)
		}
		original := bytes.Clone(zeroDateTime)
		var buf []byte
		var value any
		for _, dest := range []any{&buf, &value} {
			if err := rows.ScanColumn(driver.ScanContext{}, 0, dest); err != nil {
				t.Fatal(err)
			}
			got := buf
			if dest == &value {
				got = value.([]byte)
			}
			got[0] = 'x'
			if !bytes.Equal(zeroDateTime, original) {
				copy(zeroDateTime, original)
				t.Fatal("destination aliases shared zero datetime")
			}
		}
	}
}

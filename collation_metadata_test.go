// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2026 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"testing"
)

type collationConnector struct{ conn *mysqlConn }

func (c collationConnector) Connect(context.Context) (driver.Conn, error) { return c.conn, nil }
func (c collationConnector) Driver() driver.Driver                        { return &MySQLDriver{} }

// ColumnDefinition41 carries a two-byte collation, unlike the handshake.
// 319 is utf8mb4_bg_0900_as_cs, not the binary pseudo-charset (63).
func TestCollationMetadataPublic(t *testing.T) {
	for _, prepared := range []bool{false, true} {
		for _, collation := range []uint16{33, 63, 255, 309, 319} {
			for _, nullable := range []bool{false, true} {
				for _, field := range []struct {
					typ          fieldType
					text, binary string
				}{
					{fieldTypeString, "CHAR", "BINARY"},
					{fieldTypeVarString, "VARCHAR", "VARBINARY"},
					{fieldTypeBLOB, "TEXT", "BLOB"},
				} {
					t.Run(fmt.Sprintf("prepared=%v/collation=%d/nullable=%v/type=%s", prepared, collation, nullable, field.text), func(t *testing.T) {
						conn, mc := newRWMockConn(0)
						mc.cfg.CheckConnLiveness = false
						packet := func(seq byte, payload []byte) []byte {
							return append([]byte{byte(len(payload)), 0, 0, seq}, payload...)
						}
						flags := byte(flagNotNULL)
						if nullable {
							flags = 0
						}
						column := []byte{3, 'd', 'e', 'f', 0, 0, 0, 1, 'v', 0, 12, byte(collation), byte(collation >> 8), 16, 0, 0, 0, byte(field.typ), flags, 0, 0, 0, 0}
						eof := []byte{0xfe, 0, 0, 2, 0}
						stream := packet(1, []byte{1})
						stream = append(stream, packet(2, column)...)
						stream = append(stream, packet(3, eof)...)
						row := []byte{3, 'a', 'b', 'c'}
						if prepared {
							row = append([]byte{0, 0}, row...)
						}
						stream = append(stream, packet(4, row)...)
						stream = append(stream, packet(5, eof)...)
						conn.queuedReplies = [][]byte{stream}
						db := sql.OpenDB(collationConnector{mc})
						defer db.Close()
						var rows *sql.Rows
						var err error
						if prepared {
							prepare := packet(1, []byte{0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0})
							prepare = append(prepare, packet(2, column)...)
							prepare = append(prepare, packet(3, eof)...)
							conn.queuedReplies = [][]byte{prepare, stream}
							stmt, prepareErr := db.PrepareContext(context.Background(), "SELECT v")
							if prepareErr != nil {
								t.Fatal(prepareErr)
							}
							defer stmt.Close()
							rows, err = stmt.QueryContext(context.Background())
						} else {
							rows, err = db.QueryContext(context.Background(), "SELECT v")
						}
						if err != nil {
							t.Fatal(err)
						}
						defer rows.Close()
						types, err := rows.ColumnTypes()
						if err != nil {
							t.Fatal(err)
						}
						wantName, wantScan := field.text, reflect.TypeFor[string]()
						if nullable {
							wantScan = reflect.TypeFor[sql.NullString]()
						}
						if collation == 63 {
							wantName, wantScan = field.binary, reflect.TypeFor[[]byte]()
						}
						if got := types[0].DatabaseTypeName(); got != wantName {
							t.Errorf("DatabaseTypeName=%s; want %s", got, wantName)
						}
						if got := types[0].ScanType(); got != wantScan {
							t.Errorf("ScanType=%v; want %v", got, wantScan)
						}
						if !rows.Next() {
							t.Fatalf("Next: %v", rows.Err())
						}
						var value string
						if err := rows.Scan(&value); err != nil {
							t.Fatal(err)
						}
						if value != "abc" {
							t.Fatalf("Scan=%q", value)
						}
						if rows.Next() || rows.Err() != nil {
							t.Fatalf("unexpected extra row/error: %v", rows.Err())
						}
					})
				}
			}
		}
	}
}

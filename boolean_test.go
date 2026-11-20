// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2026 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"database/sql"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestTinyInt1IsBoolConfig(t *testing.T) {
	cfg := NewConfig()
	if cfg.tinyInt1IsBool {
		t.Fatal("tinyInt1IsBool should be disabled by default")
	}
	if got := cfg.FormatDSN(); strings.Contains(got, "tinyInt1IsBool") {
		t.Fatalf("FormatDSN() = %q; default option should be omitted", got)
	}

	if err := cfg.Apply(TinyInt1IsBool(true)); err != nil {
		t.Fatal(err)
	}
	if !cfg.tinyInt1IsBool {
		t.Fatal("TinyInt1IsBool(true) did not enable the option")
	}
	if got := cfg.FormatDSN(); !strings.Contains(got, "tinyInt1IsBool=true") {
		t.Fatalf("FormatDSN() = %q; want tinyInt1IsBool=true", got)
	}
	parsed, err := ParseDSN(cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	if !parsed.tinyInt1IsBool {
		t.Fatal("FormatDSN did not preserve TinyInt1IsBool(true)")
	}
	if err := cfg.Apply(TinyInt1IsBool(false)); err != nil {
		t.Fatal(err)
	}
	if cfg.tinyInt1IsBool {
		t.Fatal("TinyInt1IsBool(false) did not disable the option")
	}
	if got := cfg.FormatDSN(); strings.Contains(got, "tinyInt1IsBool") {
		t.Fatalf("FormatDSN() = %q; disabled option should be omitted", got)
	}

	for _, tt := range []struct {
		dsn  string
		want bool
	}{
		{"/", false},
		{"/?tinyInt1IsBool=false", false},
		{"/?tinyInt1IsBool=true", true},
	} {
		t.Run(tt.dsn, func(t *testing.T) {
			cfg, err := ParseDSN(tt.dsn)
			if err != nil {
				t.Fatal(err)
			}
			if cfg.tinyInt1IsBool != tt.want {
				t.Fatalf("ParseDSN() tinyInt1IsBool = %v; want %v", cfg.tinyInt1IsBool, tt.want)
			}
			formatted := cfg.FormatDSN()
			if strings.Contains(formatted, "tinyInt1IsBool") != tt.want {
				t.Fatalf("FormatDSN() = %q; option should be present only when enabled", formatted)
			}
			roundTrip, err := ParseDSN(formatted)
			if err != nil {
				t.Fatal(err)
			}
			if roundTrip.tinyInt1IsBool != tt.want {
				t.Fatalf("DSN round trip tinyInt1IsBool = %v; want %v", roundTrip.tinyInt1IsBool, tt.want)
			}
		})
	}

	if _, err := ParseDSN("/?tinyInt1IsBool=invalid"); err == nil {
		t.Fatal("ParseDSN accepted invalid tinyInt1IsBool value")
	}
}

func TestTinyInt1IsBool(t *testing.T) {
	runTestsParallel(t, dsn+"&tinyInt1IsBool=true", func(dbt *DBTest, tbl string) {
		dbt.mustExec("CREATE TABLE " + tbl + " (" +
			"id INT PRIMARY KEY, " +
			"b TINYINT(1) NOT NULL, " +
			"bn TINYINT(1), " +
			"n TINYINT(2) NOT NULL, " +
			"u TINYINT(1) UNSIGNED NOT NULL)")
		dbt.mustExec("INSERT INTO " + tbl + " VALUES " +
			"(1, 0, NULL, 2, 1), " +
			"(2, 1, 0, 2, 1), " +
			"(3, 2, -1, 2, 1), " +
			"(4, 0, 0, 2, 1)")

		rows := dbt.mustQuery("SELECT b, bn, n, u FROM " + tbl + " ORDER BY id")
		defer rows.Close()

		columnTypes, err := rows.ColumnTypes()
		if err != nil {
			dbt.Fatal(err)
		}
		wantDatabaseTypes := []string{"BOOLEAN", "BOOLEAN", "TINYINT", "UNSIGNED TINYINT"}
		wantScanTypes := []reflect.Type{
			reflect.TypeFor[bool](),
			reflect.TypeFor[sql.NullBool](),
			scanTypeInt8,
			scanTypeUint8,
		}
		for i, columnType := range columnTypes {
			if got := columnType.DatabaseTypeName(); got != wantDatabaseTypes[i] {
				dbt.Errorf("column %d DatabaseTypeName() = %q; want %q", i, got, wantDatabaseTypes[i])
			}
			if got := columnType.ScanType(); got != wantScanTypes[i] {
				dbt.Errorf("column %d ScanType() = %v; want %v", i, got, wantScanTypes[i])
			}
		}

		want := [][4]any{
			{false, nil, int64(2), int64(1)},
			{true, false, int64(2), int64(1)},
			{true, true, int64(2), int64(1)},
			{false, false, int64(2), int64(1)},
		}
		row := 0
		for ; rows.Next(); row++ {
			var got [4]any
			if err := rows.Scan(&got[0], &got[1], &got[2], &got[3]); err != nil {
				dbt.Fatal(err)
			}
			if row >= len(want) {
				dbt.Errorf("unexpected row %d = %#v", row, got)
				continue
			}
			if !reflect.DeepEqual(got, want[row]) {
				dbt.Errorf("row %d = %#v; want %#v", row, got, want[row])
			}
		}
		if err := rows.Err(); err != nil {
			dbt.Fatal(err)
		}
		if row != len(want) {
			dbt.Errorf("got %d rows; want %d", row, len(want))
		}

		stmt, err := dbt.db.Prepare("SELECT b, bn, n, u FROM " + tbl + " WHERE id = ?")
		if err != nil {
			dbt.Fatal(err)
		}
		defer stmt.Close()

		for _, id := range []int{3, 4} {
			var got [4]any
			if err := stmt.QueryRow(id).Scan(&got[0], &got[1], &got[2], &got[3]); err != nil {
				dbt.Fatal(err)
			}
			if !reflect.DeepEqual(got, want[id-1]) {
				dbt.Errorf("prepared statement row %d = %#v; want %#v", id, got, want[id-1])
			}
		}
	})
}

func TestTinyInt1IsBoolDisabled(t *testing.T) {
	for _, tt := range []struct{ name, params string }{
		{"default", ""},
		{"explicit false", "&tinyInt1IsBool=false"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			runTestsParallel(t, dsn+tt.params, func(dbt *DBTest, tbl string) {
				dbt.mustExec("CREATE TABLE " + tbl + " (b TINYINT(1) NOT NULL, bn TINYINT(1))")
				dbt.mustExec("INSERT INTO " + tbl + " (b) VALUES (-128), (-1), (0), (1), (5), (127)")
				query := "SELECT b, b, b, b, b, b, bn FROM " + tbl + " ORDER BY b"
				stmt, err := dbt.db.Prepare(query)
				if err != nil {
					dbt.Fatal(err)
				}
				defer stmt.Close()
				for _, prepared := range []bool{false, true} {
					var rows *sql.Rows
					if prepared {
						rows, err = stmt.Query()
					} else {
						rows, err = dbt.db.Query(query)
					}
					if err != nil {
						dbt.Fatal(err)
					}
					defer rows.Close()
					types, err := rows.ColumnTypes()
					if err != nil {
						dbt.Fatal(err)
					}
					for i, column := range types {
						wantType := scanTypeInt8
						if i == 6 {
							wantType = scanTypeNullInt
						}
						if column.DatabaseTypeName() != "TINYINT" || column.ScanType() != wantType {
							dbt.Fatalf("prepared=%v column=%d: got %s/%v; want TINYINT/%v", prepared, i, column.DatabaseTypeName(), column.ScanType(), wantType)
						}
					}
					for _, want := range []int64{-128, -1, 0, 1, 5, 127} {
						if !rows.Next() {
							dbt.Fatalf("prepared=%v: missing row for %d: %v", prepared, want, rows.Err())
						}
						var n int
						var n8 int8
						var n64 int64
						var nullable sql.NullInt64
						var anyValue any
						var text string
						null := sql.NullInt64{Int64: 5, Valid: true}
						if err := rows.Scan(&n, &n8, &n64, &nullable, &anyValue, &text, &null); err != nil {
							dbt.Fatal(err)
						}
						if int64(n) != want || int64(n8) != want || n64 != want || nullable != (sql.NullInt64{Int64: want, Valid: true}) || anyValue != want || text != strconv.FormatInt(want, 10) || null != (sql.NullInt64{}) {
							dbt.Fatalf("prepared=%v: got %d, %d, %d, %+v, %#v, %q, %+v; want numeric %d and NULL", prepared, n, n8, n64, nullable, anyValue, text, null, want)
						}
					}
					if rows.Next() {
						dbt.Fatal("unexpected extra row")
					}
					if err := rows.Err(); err != nil {
						dbt.Fatal(err)
					}
				}
			})
		})
	}
}

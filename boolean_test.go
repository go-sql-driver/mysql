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
	"strings"
	"testing"
)

func TestTinyInt1IsBoolConfig(t *testing.T) {
	cfg := NewConfig()
	if !cfg.tinyInt1IsBool {
		t.Fatal("tinyInt1IsBool should be enabled by default")
	}
	if got := cfg.FormatDSN(); strings.Contains(got, "tinyInt1IsBool") {
		t.Fatalf("FormatDSN() = %q; default option should be omitted", got)
	}

	if err := cfg.Apply(TinyInt1IsBool(false)); err != nil {
		t.Fatal(err)
	}
	if cfg.tinyInt1IsBool {
		t.Fatal("TinyInt1IsBool(false) did not disable the option")
	}
	if got := cfg.FormatDSN(); !strings.Contains(got, "tinyInt1IsBool=false") {
		t.Fatalf("FormatDSN() = %q; want tinyInt1IsBool=false", got)
	}

	cfg, err := ParseDSN("/?tinyInt1IsBool=false")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.tinyInt1IsBool {
		t.Fatal("ParseDSN did not disable tinyInt1IsBool")
	}

	if _, err := ParseDSN("/?tinyInt1IsBool=invalid"); err == nil {
		t.Fatal("ParseDSN accepted invalid tinyInt1IsBool value")
	}
}

func TestTinyInt1IsBool(t *testing.T) {
	runTestsParallel(t, dsn, func(dbt *DBTest, tbl string) {
		dbt.mustExec("CREATE TABLE " + tbl + " (" +
			"id INT PRIMARY KEY, " +
			"b TINYINT(1) NOT NULL, " +
			"bn TINYINT(1), " +
			"n TINYINT(2) NOT NULL, " +
			"u TINYINT(1) UNSIGNED NOT NULL)")
		dbt.mustExec("INSERT INTO " + tbl + " VALUES " +
			"(1, 0, NULL, 2, 1), " +
			"(2, 1, 0, 2, 1), " +
			"(3, 2, -1, 2, 1)")

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
		}
		for row := 0; rows.Next(); row++ {
			var got [4]any
			if err := rows.Scan(&got[0], &got[1], &got[2], &got[3]); err != nil {
				dbt.Fatal(err)
			}
			if !reflect.DeepEqual(got, want[row]) {
				dbt.Errorf("row %d = %#v; want %#v", row, got, want[row])
			}
		}
		if err := rows.Err(); err != nil {
			dbt.Fatal(err)
		}

		stmt, err := dbt.db.Prepare("SELECT b, bn, n, u FROM " + tbl + " WHERE id = ?")
		if err != nil {
			dbt.Fatal(err)
		}
		defer stmt.Close()

		var got [4]any
		if err := stmt.QueryRow(3).Scan(&got[0], &got[1], &got[2], &got[3]); err != nil {
			dbt.Fatal(err)
		}
		if !reflect.DeepEqual(got, want[2]) {
			dbt.Errorf("prepared statement row = %#v; want %#v", got, want[2])
		}
	})
}

func TestTinyInt1IsBoolDisabled(t *testing.T) {
	runTestsParallel(t, dsn+"&tinyInt1IsBool=false", func(dbt *DBTest, tbl string) {
		dbt.mustExec("CREATE TABLE " + tbl + " (b TINYINT(1) NOT NULL)")
		dbt.mustExec("INSERT INTO " + tbl + " VALUES (2)")

		var got any
		if err := dbt.db.QueryRow("SELECT b FROM " + tbl).Scan(&got); err != nil {
			dbt.Fatal(err)
		}
		if got != int64(2) {
			dbt.Fatalf("Scan(&any) = %#v; want int64(2)", got)
		}
	})
}

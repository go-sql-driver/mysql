// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2026 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

// This file tests the converter from outside the package, because exporting it
// exists so consumers can use it: a wrapper that implements
// driver.NamedValueChecker without owning a connection cannot reach an
// unexported type.

package mysql_test

import (
	"database/sql/driver"
	"testing"

	"github.com/go-sql-driver/mysql"
)

func TestMySQLConverterIsValueConverter(t *testing.T) {
	var _ driver.ValueConverter = mysql.MySQLConverter{}
}

func TestMySQLConverterSupportsHighBitUint64(t *testing.T) {
	want := uint64(1<<63 + 7)

	got, err := mysql.MySQLConverter{}.ConvertValue(want)
	if err != nil {
		t.Fatal("high-bit uint64 not convertible:", err)
	}

	if got != want {
		t.Fatalf("high-bit uint64 not preserved, got %#v %T", got, got)
	}
}

func TestMySQLConverterRefusesUnsupportedSlice(t *testing.T) {
	// The one deviation from driver.DefaultParameterConverter is the uint64
	// support; everything else, such as a []string, must still be refused.
	if _, err := (mysql.MySQLConverter{}).ConvertValue([]string{"v"}); err == nil {
		t.Fatal("expected an error for an unsupported slice")
	}
}

// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2016 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"database/sql/driver"
	"testing"
	"time"
)

func TestInterpolateParams(t *testing.T) {
	mc := &mysqlConn{
		buf:              newBuffer(nil),
		maxAllowedPacket: maxPacketSize,
		cfg: &Config{
			InterpolateParams: true,
		},
	}

	q, err := mc.interpolateParams("SELECT ?+?", []driver.Value{int64(42), "gopher"})
	if err != nil {
		t.Errorf("Expected err=nil, got %#v", err)
		return
	}
	expected := `SELECT 42+'gopher'`
	if q != expected {
		t.Errorf("Expected: %q\nGot: %q", expected, q)
	}
}

func TestInterpolateParamsTooManyPlaceholders(t *testing.T) {
	mc := &mysqlConn{
		buf:              newBuffer(nil),
		maxAllowedPacket: maxPacketSize,
		cfg: &Config{
			InterpolateParams: true,
		},
	}

	q, err := mc.interpolateParams("SELECT ?+?", []driver.Value{int64(42)})
	if err != driver.ErrSkip {
		t.Errorf("Expected err=driver.ErrSkip, got err=%#v, q=%#v", err, q)
	}
}

// We don't support placeholder in string literal for now.
// https://github.com/go-sql-driver/mysql/pull/490
func TestInterpolateParamsPlaceholderInString(t *testing.T) {
	mc := &mysqlConn{
		buf:              newBuffer(nil),
		maxAllowedPacket: maxPacketSize,
		cfg: &Config{
			InterpolateParams: true,
		},
	}

	q, err := mc.interpolateParams("SELECT 'abc?xyz',?", []driver.Value{int64(42)})
	// When InterpolateParams support string literal, this should return `"SELECT 'abc?xyz', 42`
	if err != driver.ErrSkip {
		t.Errorf("Expected err=driver.ErrSkip, got err=%#v, q=%#v", err, q)
	}
}

func TestIoWriteTimeout(t *testing.T) {
	var err error

	if !available {
		t.Skipf("MySQL server not running on %s", netAddr)
	}

	mySqlDriver := MySQLDriver{}
	conn, err := mySqlDriver.Open(dsn)
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}

	mc, ok := conn.(*mysqlConn)
	if !ok {
		t.Fatalf("It can't type-assert driver.conn is of mysqlConn")
	}
	mc.writeTimeout, err = time.ParseDuration("1ns")
	if !ok {
		t.Fatalf("err(%s)", err)
	}

	execer, ok := conn.(driver.Execer)
	if !ok {
		t.Fatalf("It can't type-assert driver.conn is of driver.Execer")
	}

	_, err = execer.Exec("show databases", nil)
	if err == nil {
		return
	}
	if err != driver.ErrBadConn {
		t.Fatalf("err must be driver.ErrBadConn. err(%s)", err)
	}
}

func TestIoReadTimeout(t *testing.T) {
	var err error

	if !available {
		t.Skipf("MySQL server not running on %s", netAddr)
	}

	mySqlDriver := MySQLDriver{}
	conn, err := mySqlDriver.Open(dsn)
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}

	mc, ok := conn.(*mysqlConn)
	if !ok {
		t.Fatalf("It can't type-assert driver.conn is of mysqlConn")
	}
	mc.buf.timeout, err = time.ParseDuration("1ns")
	if !ok {
		t.Fatalf("err(%s)", err)
	}

	execer, ok := conn.(driver.Execer)
	if !ok {
		t.Fatalf("It can't type-assert driver.conn is of driver.Execer")
	}

	_, err = execer.Exec("show databases", nil)
	if err == nil {
		return
	}
	if err != driver.ErrBadConn {
		t.Fatalf("err must be driver.ErrBadConn. err(%s)", err)
	}
}

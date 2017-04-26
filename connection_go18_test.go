// +build go1.8

// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2017 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"context"
	"database/sql/driver"
	"testing"
	"time"
)

func TestPingNilContext(t *testing.T) {
	var err error

	if !available {
		t.Skipf("MySQL server not running on %s", netAddr)
	}

	mySqlDriver := MySQLDriver{}
	conn, err := mySqlDriver.Open(dsn)
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}

	pinger, ok := conn.(driver.Pinger)
	if !ok {
		t.Fatalf("It can't type-assert driver.conn is of driver.Pinger")
	}

	err = pinger.Ping(nil)
	if err != nil {
		t.Fatalf("err must be nil. err(%s)", err)
	}
}

func TestPingBackgroundContext(t *testing.T) {
	var err error

	if !available {
		t.Skipf("MySQL server not running on %s", netAddr)
	}

	mySqlDriver := MySQLDriver{}
	conn, err := mySqlDriver.Open(dsn)
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}

	pinger, ok := conn.(driver.Pinger)
	if !ok {
		t.Fatalf("It can't type-assert driver.conn is of driver.Pinger")
	}

	err = pinger.Ping(context.Background())
	if err != nil {
		t.Fatalf("err must be nil. err(%s)", err)
	}
}

func TestPingMySqlCtx(t *testing.T) {
	var err error

	if !available {
		t.Skipf("MySQL server not running on %s", netAddr)
	}

	mySqlDriver := MySQLDriver{}
	conn, err := mySqlDriver.Open(dsn)
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}

	pinger, ok := conn.(driver.Pinger)
	if !ok {
		t.Fatalf("It can't type-assert driver.conn is of driver.Pinger")
	}

	err = pinger.Ping(context.Background())
	if err != nil {
		t.Fatalf("err must be nil. err(%s)", err)
	}

	mc, ok := conn.(*mysqlConn)
	if !ok {
		t.Fatalf("It can't type-assert driver.conn is of mysqlConn")
	}

	if mc.ctx != nil {
		t.Fatalf("mc.ctx must be nil after Ping(). mc.ctx(%s)", mc.ctx)
	}
}

func TestPingCtxDone(t *testing.T) {
	var err error

	if !available {
		t.Skipf("MySQL server not running on %s", netAddr)
	}

	mySqlDriver := MySQLDriver{}
	conn, err := mySqlDriver.Open(dsn)
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}

	pinger, ok := conn.(driver.Pinger)
	if !ok {
		t.Fatalf("It can't type-assert driver.conn is of driver.Pinger")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = pinger.Ping(ctx)
	if err == nil {
		return
	}
	if err != driver.ErrBadConn {
		t.Fatalf("err must be driver.ErrBadConn. err(%s)", err)
	}
}

func TestPingCtxDoneIoWriteTimeout(t *testing.T) {
	var err error

	if !available {
		t.Skipf("MySQL server not running on %s", netAddr)
	}

	mySqlDriver := MySQLDriver{}
	conn, err := mySqlDriver.Open(dsn)
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}

	pinger, ok := conn.(driver.Pinger)
	if !ok {
		t.Fatalf("It can't type-assert driver.conn is of driver.Pinger")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	mc, ok := conn.(*mysqlConn)
	if !ok {
		t.Fatalf("It can't type-assert driver.conn is of mysqlConn")
	}
	mc.writeTimeout, err = time.ParseDuration("1ns")
	if !ok {
		t.Fatalf("err(%s)", err)
	}

	err = pinger.Ping(ctx)
	if err == nil {
		return
	}
	if err != driver.ErrBadConn {
		t.Fatalf("err must be driver.ErrBadConn. err(%s)", err)
	}
}

func TestPingCtxDoneIoReadTimeout(t *testing.T) {
	var err error

	if !available {
		t.Skipf("MySQL server not running on %s", netAddr)
	}

	mySqlDriver := MySQLDriver{}
	conn, err := mySqlDriver.Open(dsn)
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}

	pinger, ok := conn.(driver.Pinger)
	if !ok {
		t.Fatalf("It can't type-assert driver.conn is of driver.Pinger")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	mc, ok := conn.(*mysqlConn)
	if !ok {
		t.Fatalf("It can't type-assert driver.conn is of mysqlConn")
	}
	mc.buf.timeout, err = time.ParseDuration("1ns")
	if !ok {
		t.Fatalf("err(%s)", err)
	}

	err = pinger.Ping(ctx)
	if err == nil {
		return
	}
	if err != driver.ErrBadConn {
		t.Fatalf("err must be driver.ErrBadConn. err(%s)", err)
	}
}

func TestPingCtxDeadline(t *testing.T) {
	var err error

	if !available {
		t.Skipf("MySQL server not running on %s", netAddr)
	}

	mySqlDriver := MySQLDriver{}
	conn, err := mySqlDriver.Open(dsn)
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}

	pinger, ok := conn.(driver.Pinger)
	if !ok {
		t.Fatalf("It can't type-assert driver.conn is of driver.Pinger")
	}

	ctx, _ := context.WithTimeout(context.Background(), 0)

	err = pinger.Ping(ctx)
	if err == nil {
		return
	}
	if err != driver.ErrBadConn {
		t.Fatalf("err must be driver.ErrBadConn. err(%s)", err)
	}
}

func TestPingCtxDeadlineAfterIoWriteTimeout(t *testing.T) {
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

	pinger, ok := conn.(driver.Pinger)
	if !ok {
		t.Fatalf("It can't type-assert driver.conn is of driver.Pinger")
	}

	ctx, _ := context.WithTimeout(context.Background(), 1*time.Hour)

	err = pinger.Ping(ctx)
	if err == nil {
		return
	}
	if err != driver.ErrBadConn {
		t.Fatalf("err must be driver.ErrBadConn. err(%s)", err)
	}
}

func TestPingCtxDeadlineBeforeIoWriteTimeout(t *testing.T) {
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
	mc.writeTimeout, err = time.ParseDuration("1h")
	if !ok {
		t.Fatalf("err(%s)", err)
	}

	pinger, ok := conn.(driver.Pinger)
	if !ok {
		t.Fatalf("It can't type-assert driver.conn is of driver.Pinger")
	}

	ctx, _ := context.WithTimeout(context.Background(), 0)

	err = pinger.Ping(ctx)
	if err == nil {
		return
	}
	if err != driver.ErrBadConn {
		t.Fatalf("err must be driver.ErrBadConn. err(%s)", err)
	}
}

func TestPingCtxDeadlineAfterIoReadTimeout(t *testing.T) {
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

	pinger, ok := conn.(driver.Pinger)
	if !ok {
		t.Fatalf("It can't type-assert driver.conn is of driver.Pinger")
	}

	ctx, _ := context.WithTimeout(context.Background(), 1*time.Hour)

	err = pinger.Ping(ctx)
	if err == nil {
		return
	}
	if err != driver.ErrBadConn {
		t.Fatalf("err must be driver.ErrBadConn. err(%s)", err)
	}
}

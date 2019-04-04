// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2016 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"context"
	"database/sql/driver"
	"errors"
	"net"
	"testing"
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

func TestCheckNamedValue(t *testing.T) {
	value := driver.NamedValue{Value: ^uint64(0)}
	x := &mysqlConn{}
	err := x.CheckNamedValue(&value)

	if err != nil {
		t.Fatal("uint64 high-bit not convertible", err)
	}

	if value.Value != ^uint64(0) {
		t.Fatalf("uint64 high-bit converted, got %#v %T", value.Value, value.Value)
	}
}

// TestCleanCancel tests passed context is cancelled at start.
// No packet should be sent.  Connection should keep current status.
func TestCleanCancel(t *testing.T) {
	mc := &mysqlConn{
		closech: make(chan struct{}),
	}
	mc.startWatcher()
	defer mc.cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	for i := 0; i < 3; i++ { // Repeat same behavior
		err := mc.Ping(ctx)
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got %#v", err)
		}

		if mc.closed.IsSet() {
			t.Error("expected mc is not closed, closed actually")
		}

		if mc.watching {
			t.Error("expected watching is false, but true")
		}
	}
}

func TestPingMarkBadConnection(t *testing.T) {
	nc := badConnection{err: errors.New("boom")}
	ms := &mysqlConn{
		netConn:          nc,
		buf:              newBuffer(nc),
		maxAllowedPacket: defaultMaxAllowedPacket,
	}

	err := ms.Ping(context.Background())

	if err != driver.ErrBadConn {
		t.Errorf("expected driver.ErrBadConn, got  %#v", err)
	}
}

func TestPingErrInvalidConn(t *testing.T) {
	nc := badConnection{err: errors.New("failed to write"), n: 10}
	ms := &mysqlConn{
		netConn:          nc,
		buf:              newBuffer(nc),
		maxAllowedPacket: defaultMaxAllowedPacket,
		closech:          make(chan struct{}),
	}

	err := ms.Ping(context.Background())

	if err != ErrInvalidConn {
		t.Errorf("expected ErrInvalidConn, got  %#v", err)
	}
}

type badConnection struct {
	n   int
	err error
	net.Conn
}

func (bc badConnection) Write(b []byte) (n int, err error) {
	return bc.n, bc.err
}

func (bc badConnection) Close() error {
	return nil
}

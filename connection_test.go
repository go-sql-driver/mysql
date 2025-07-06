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
	"encoding/json"
	"errors"
	"net"
	"testing"
)

func TestInterpolateParams(t *testing.T) {
	mc := &mysqlConn{
		buf:              newBuffer(),
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

func TestInterpolateParamsJSONRawMessage(t *testing.T) {
	mc := &mysqlConn{
		buf:              newBuffer(),
		maxAllowedPacket: maxPacketSize,
		cfg: &Config{
			InterpolateParams: true,
		},
	}

	buf, err := json.Marshal(struct {
		Value int `json:"value"`
	}{Value: 42})
	if err != nil {
		t.Errorf("Expected err=nil, got %#v", err)
		return
	}
	q, err := mc.interpolateParams("SELECT ?", []driver.Value{json.RawMessage(buf)})
	if err != nil {
		t.Errorf("Expected err=nil, got %#v", err)
		return
	}
	expected := `SELECT '{\"value\":42}'`
	if q != expected {
		t.Errorf("Expected: %q\nGot: %q", expected, q)
	}
}

func TestInterpolateParamsTooManyPlaceholders(t *testing.T) {
	mc := &mysqlConn{
		buf:              newBuffer(),
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

func TestInterpolateParamsUint64(t *testing.T) {
	mc := &mysqlConn{
		buf:              newBuffer(),
		maxAllowedPacket: maxPacketSize,
		cfg: &Config{
			InterpolateParams: true,
		},
	}

	q, err := mc.interpolateParams("SELECT ?", []driver.Value{uint64(42)})
	if err != nil {
		t.Errorf("Expected err=nil, got err=%#v, q=%#v", err, q)
	}
	if q != "SELECT 42" {
		t.Errorf("Expected uint64 interpolation to work, got q=%#v", q)
	}
}

func TestCheckNamedValue(t *testing.T) {
	value := driver.NamedValue{Value: ^uint64(0)}
	mc := &mysqlConn{}
	err := mc.CheckNamedValue(&value)

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

	for range 3 { // Repeat same behavior
		err := mc.Ping(ctx)
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got %#v", err)
		}

		if mc.closed.Load() {
			t.Error("expected mc is not closed, closed actually")
		}

		if mc.watching {
			t.Error("expected watching is false, but true")
		}
	}
}

func TestPingMarkBadConnection(t *testing.T) {
	nc := badConnection{err: errors.New("boom")}
	mc := &mysqlConn{
		netConn:          nc,
		buf:              newBuffer(),
		maxAllowedPacket: defaultMaxAllowedPacket,
		closech:          make(chan struct{}),
		cfg:              NewConfig(),
	}

	err := mc.Ping(context.Background())

	if err != driver.ErrBadConn {
		t.Errorf("expected driver.ErrBadConn, got  %#v", err)
	}
}

func TestPingErrInvalidConn(t *testing.T) {
	nc := badConnection{err: errors.New("failed to write"), n: 10}
	mc := &mysqlConn{
		netConn:          nc,
		buf:              newBuffer(),
		maxAllowedPacket: defaultMaxAllowedPacket,
		closech:          make(chan struct{}),
		cfg:              NewConfig(),
	}

	err := mc.Ping(context.Background())

	if err != nc.err {
		t.Errorf("expected %#v, got  %#v", nc.err, err)
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

func TestInterpolateParamsWithComments(t *testing.T) {
	mc := &mysqlConn{
		buf:              newBuffer(),
		maxAllowedPacket: maxPacketSize,
		cfg: &Config{
			InterpolateParams: true,
		},
	}

	tests := []struct {
		query      string
		args       []driver.Value
		expected   string
		shouldSkip bool
	}{
		// ? in single-line comment (--) should not be replaced
		{"SELECT 1 -- ?\n, ?", []driver.Value{int64(42)}, "SELECT 1 -- ?\n, 42", false},
		// ? in single-line comment (#) should not be replaced
		{"SELECT 1 # ?\n, ?", []driver.Value{int64(42)}, "SELECT 1 # ?\n, 42", false},
		// ? in multi-line comment should not be replaced
		{"SELECT /* ? */ ?", []driver.Value{int64(42)}, "SELECT /* ? */ 42", false},
		// ? in string literal should not be replaced
		{"SELECT '?', ?", []driver.Value{int64(42)}, "SELECT '?', 42", false},
		// ? in backtick identifier should not be replaced
		{"SELECT `?`, ?", []driver.Value{int64(42)}, "SELECT `?`, 42", false},
		// ? in backslash-escaped string literal should not be replaced
		{"SELECT 'C:\\path\\?x.txt', ?", []driver.Value{int64(42)}, "SELECT 'C:\\path\\?x.txt', 42", false},
		// ? in backslash-escaped string literal should not be replaced
		{"SELECT '\\'?', col FROM tbl WHERE id = ? AND desc = 'foo\\'bar?'", []driver.Value{int64(42)}, "SELECT '\\'?', col FROM tbl WHERE id = 42 AND desc = 'foo\\'bar?'", false},
		// Multiple comments and real placeholders
		{"SELECT ? -- comment ?\n, ? /* ? */ , ? # ?\n, ?", []driver.Value{int64(1), int64(2), int64(3)}, "SELECT 1 -- comment ?\n, 2 /* ? */ , 3 # ?\n, ?", true},
	}

	for i, test := range tests {

		q, err := mc.interpolateParams(test.query, test.args)
		if test.shouldSkip {
			if err != driver.ErrSkip {
				t.Errorf("Test %d: Expected driver.ErrSkip, got err=%#v, q=%#v", i, err, q)
			}
			continue
		}
		if err != nil {
			t.Errorf("Test %d: Expected err=nil, got %#v", i, err)
			continue
		}
		if q != test.expected {
			t.Errorf("Test %d: Expected: %q\nGot: %q", i, test.expected, q)
		}
	}
}

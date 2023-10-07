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
	"io"
	"net"
	"testing"
	"time"
)

func TestInterpolateParams(t *testing.T) {
	mc := &mysqlConn{
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

func TestInterpolateParamsUint64(t *testing.T) {
	mc := &mysqlConn{
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

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	for i := 0; i < 3; i++ { // Repeat same behavior
		err := mc.Ping(ctx)
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got %#v", err)
		}

		if mc.closed.Load() {
			t.Error("expected mc is not closed, closed actually")
		}
	}
}

func TestPingMarkBadConnection(t *testing.T) {
	t.Run("empty write", func(t *testing.T) {
		nc := badConnection{
			werr: errors.New("boom"),
			done: make(chan struct{}),
		}
		ms := &mysqlConn{
			netConn:          nc,
			maxAllowedPacket: defaultMaxAllowedPacket,
		}
		ms.startGoroutines()
		defer ms.cleanup()

		err := ms.Ping(context.Background())

		if err != driver.ErrBadConn {
			t.Errorf("expected driver.ErrBadConn, got  %#v", err)
		}
	})

	t.Run("unexpected read", func(t *testing.T) {
		nc := badConnection{
			rerr: io.EOF,
			read: make(chan struct{}, 1),
			done: make(chan struct{}),
		}
		ms := &mysqlConn{
			netConn:          nc,
			maxAllowedPacket: defaultMaxAllowedPacket,
		}
		ms.startGoroutines()
		defer ms.cleanup()

		<-nc.read
		err := ms.Ping(context.Background())

		if err != driver.ErrBadConn {
			t.Errorf("expected driver.ErrBadConn, got  %#v", err)
		}
	})
}

func TestPingErrInvalidConn(t *testing.T) {
	nc := badConnection{
		werr: errors.New("failed to write"),
		n:    10,
		done: make(chan struct{}),
	}
	ms := &mysqlConn{
		netConn:          nc,
		maxAllowedPacket: defaultMaxAllowedPacket,
		closech:          make(chan struct{}),
		cfg:              NewConfig(),
	}
	ms.startGoroutines()
	defer ms.cleanup()

	err := ms.Ping(context.Background())

	if err != ErrInvalidConn {
		t.Errorf("expected ErrInvalidConn, got  %#v", err)
	}
}

type badConnection struct {
	n    int
	rerr error
	werr error
	read chan struct{}
	done chan struct{}
}

func (bc badConnection) Read(b []byte) (n int, err error) {
	select {
	case bc.read <- struct{}{}:
	case <-bc.done:
		return 0, io.EOF
	}

	if bc.rerr != nil {
		return 0, bc.rerr
	}
	<-bc.done
	return 0, io.EOF
}

func (bc badConnection) Write(b []byte) (n int, err error) {
	if bc.werr != nil {
		return bc.n, bc.werr
	}
	return 0, io.ErrShortWrite
}

func (bc badConnection) Close() error {
	close(bc.done)
	return nil
}

func (bc badConnection) LocalAddr() net.Addr {
	return nil
}

func (bc badConnection) RemoteAddr() net.Addr {
	return nil
}

func (bc badConnection) SetDeadline(t time.Time) error {
	return nil
}

func (bc badConnection) SetReadDeadline(t time.Time) error {
	return nil
}

func (bc badConnection) SetWriteDeadline(t time.Time) error {
	return nil
}

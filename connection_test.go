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
	"time"
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
		// 2--1: -- followed by digit is NOT a comment (it's the number 2 minus minus 1)
		{"SELECT ?--1", []driver.Value{int64(2)}, "SELECT 2--1", false},
		// /* */*: After closing block comment, */* should NOT start a new comment
		{"SELECT /* comment */* ?, ?", []driver.Value{int64(1), int64(2)}, "SELECT /* comment */* 1, 2", false},
		// /* */*: More complex case with actual comment after
		{"SELECT /* c1 */*/* c2 */ ?, ?", []driver.Value{int64(1), int64(2)}, "SELECT /* c1 */*/* c2 */ 1, 2", false},
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

// chunkedConn is a net.Conn that serves pre-built data chunks, one per Read
// call. This simulates the behavior seen with TLS connections, where the
// server's TLS library typically produces a separate TLS record per write
// and Go's crypto/tls.Read returns one record at a time.
type chunkedConn struct {
	chunks [][]byte
	idx    int // current chunk index
	off    int // offset within current chunk
}

func (c *chunkedConn) Read(b []byte) (int, error) {
	if c.idx >= len(c.chunks) {
		return 0, errors.New("no more data")
	}
	n := copy(b, c.chunks[c.idx][c.off:])
	c.off += n
	if c.off >= len(c.chunks[c.idx]) {
		c.idx++
		c.off = 0
	}
	return n, nil
}

func (c *chunkedConn) Write(b []byte) (int, error)        { return len(b), nil } // swallow writes (e.g. COM_QUERY)
func (c *chunkedConn) Close() error                       { return nil }
func (c *chunkedConn) LocalAddr() net.Addr                { return nil }
func (c *chunkedConn) RemoteAddr() net.Addr               { return nil }
func (c *chunkedConn) SetDeadline(_ time.Time) error      { return nil }
func (c *chunkedConn) SetReadDeadline(_ time.Time) error  { return nil }
func (c *chunkedConn) SetWriteDeadline(_ time.Time) error { return nil }

var _ net.Conn = (*chunkedConn)(nil)

// makePacket wraps a payload in a MySQL protocol packet header.
func makePacket(seq byte, payload []byte) []byte {
	pkt := make([]byte, 4+len(payload))
	pkt[0] = byte(len(payload))
	pkt[1] = byte(len(payload) >> 8)
	pkt[2] = byte(len(payload) >> 16)
	pkt[3] = seq
	copy(pkt[4:], payload)
	return pkt
}

// TestGetSystemVarBufferReuse verifies that getSystemVar returns a value that
// is not corrupted by the subsequent skipRows call.
//
// The row value returned by readRow points into the read buffer. skipRows may
// call fill(), which overwrites that memory. The test feeds each protocol
// packet as a separate Read call via chunkedConn (mimicking TLS record
// boundaries), guaranteeing that fill() is called for the trailing EOF.
func TestGetSystemVarBufferReuse(t *testing.T) {
	// Protocol response for: SELECT @@max_allowed_packet → "67108864"
	//
	// Sequence numbers start at 1 (client sent COM_QUERY as seq 0).
	//
	//   seq 1: column count = 1
	//   seq 2: column definition (minimal valid)
	//   seq 3: EOF (end of column defs)
	//   seq 4: row data — length-encoded string "67108864"
	//   seq 5: EOF (end of rows)

	colCountPkt := makePacket(1, []byte{0x01})

	colDef := []byte{
		0x03, 'd', 'e', 'f', // catalog = "def"
		0x00, // schema = ""
		0x00, // table = ""
		0x00, // org_table = ""
		0x14, // name length = 20
		'@', '@', 'm', 'a', 'x', '_', 'a', 'l', 'l', 'o',
		'w', 'e', 'd', '_', 'p', 'a', 'c', 'k', 'e', 't',
		0x00,       // org_name = ""
		0x0c,       // length of fixed fields
		0x3f, 0x00, // charset = 63 (binary)
		0x14, 0x00, 0x00, 0x00, // column_length = 20
		0x0f,       // type = FIELD_TYPE_VARCHAR
		0x00, 0x00, // flags
		0x00,       // decimals
		0x00, 0x00, // filler
	}
	colDefPkt := makePacket(2, colDef)

	eof1 := makePacket(3, []byte{0xfe, 0x00, 0x00, 0x02, 0x00})

	// Row: length-encoded string "67108864" (8 bytes → length prefix 0x08)
	rowPkt := makePacket(4, []byte{0x08, '6', '7', '1', '0', '8', '8', '6', '4'})

	eof2 := makePacket(5, []byte{0xfe, 0x00, 0x00, 0x02, 0x00})

	// Each packet arrives in its own Read call, simulating TLS record
	// boundaries where each server Write becomes a separate TLS record
	// and each client Read returns exactly one record.
	conn := &chunkedConn{chunks: [][]byte{colCountPkt, colDefPkt, eof1, rowPkt, eof2}}

	mc := &mysqlConn{
		netConn:          conn,
		buf:              newBuffer(),
		cfg:              NewConfig(),
		closech:          make(chan struct{}),
		maxAllowedPacket: defaultMaxAllowedPacket,
		sequence:         1, // after COM_QUERY (seq 0)
	}

	val, err := mc.getSystemVar("max_allowed_packet")
	if err != nil {
		t.Fatalf("getSystemVar failed: %v", err)
	}

	const expected = "67108864"
	if val != expected {
		t.Fatalf("getSystemVar(max_allowed_packet) = %q, want %q", val, expected)
	}
}

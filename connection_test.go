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
	"reflect"
	"slices"
	"strings"
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

func TestInterpolateParamsJSONRawMessageNil(t *testing.T) {
	mc := &mysqlConn{
		buf:              newBuffer(),
		maxAllowedPacket: maxPacketSize,
		cfg: &Config{
			InterpolateParams: true,
		},
	}

	// A typed nil json.RawMessage must be interpolated as NULL, not as an
	// empty quoted string. See https://github.com/go-sql-driver/mysql/issues/1781
	q, err := mc.interpolateParams("SELECT ?", []driver.Value{json.RawMessage(nil)})
	if err != nil {
		t.Errorf("Expected err=nil, got %#v", err)
		return
	}
	expected := `SELECT NULL`
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

// Read and SetDeadline let a failed write look for a pending server error.
func (bc badConnection) Read(b []byte) (n int, err error) {
	return 0, io.EOF
}

func (bc badConnection) SetDeadline(time.Time) error {
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
		// -- followed by any control char or DEL starts a comment, as in the server
		{"SELECT 1 --\v?\n, ?", []driver.Value{int64(42)}, "SELECT 1 --\v?\n, 42", false},
		{"SELECT 1 --\f?\n, ?", []driver.Value{int64(42)}, "SELECT 1 --\f?\n, 42", false},
		{"SELECT 1 --\x01?\n, ?", []driver.Value{int64(42)}, "SELECT 1 --\x01?\n, 42", false},
		{"SELECT 1 --\x7f?\n, ?", []driver.Value{int64(42)}, "SELECT 1 --\x7f?\n, 42", false},
		// Raw 0xa0 is whitespace in latin1.
		{"SELECT 1 --\xa0?\n, ?", []driver.Value{int64(42)}, "SELECT 1 --\xa0?\n, 42", false},
		// A quote after --\xa0 must not shift the interpolation state.
		{"SELECT 1 --\xa0 don't\n, '?', ?", []driver.Value{int64(42)}, "SELECT 1 --\xa0 don't\n, '?', 42", false},
		// A quote in such a comment must not start a string literal, otherwise
		// the next '?' inside a real string literal would be replaced.
		{"SELECT ? --\f don't\n, '?'", []driver.Value{int64(42)}, "SELECT 42 --\f don't\n, '?'", false},
		// /* */*: After closing block comment, */* should NOT start a new comment
		{"SELECT /* comment */* ?, ?", []driver.Value{int64(1), int64(2)}, "SELECT /* comment */* 1, 2", false},
		// /* */*: More complex case with actual comment after
		{"SELECT /* c1 */*/* c2 */ ?, ?", []driver.Value{int64(1), int64(2)}, "SELECT /* c1 */*/* c2 */ 1, 2", false},
		// /*/: the '*' that opens a block comment must not also close it
		{"SELECT /*/ ? */ ?", []driver.Value{int64(42)}, "SELECT /*/ ? */ 42", false},
		{"SELECT /*/ ' */ ?, '?'", []driver.Value{int64(42)}, "SELECT /*/ ' */ 42, '?'", false},
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

// TestExecErrorResult checks that Exec returns the partial Result with a
// server error: one entry per result read, the failing statement's last.
func TestExecErrorResult(t *testing.T) {
	for _, tc := range []struct {
		name    string
		data    []byte
		errno   uint16
		want    []int64
		wantIDs []int64
	}{{
		name: "first",
		data: []byte{
			// ERR 1062
			0x09, 0x00, 0x00, 0x01, 0xff, 0x26, 0x04, '#', '2', '3', '0', '0', '0',
		},
		errno:   1062,
		want:    []int64{0},
		wantIDs: []int64{0},
	}, {
		name: "second",
		data: []byte{
			// OK, 1 row affected, last insert ID 5, SERVER_MORE_RESULTS_EXISTS
			0x07, 0x00, 0x00, 0x01, 0x00, 0x01, 0x05, 0x08, 0x00, 0x00, 0x00,
			// ERR 1062
			0x09, 0x00, 0x00, 0x02, 0xff, 0x26, 0x04, '#', '2', '3', '0', '0', '0',
		},
		errno:   1062,
		want:    []int64{1, 0},
		wantIDs: []int64{5, 0},
	}, {
		name: "select rows",
		data: []byte{
			// column count 1
			0x01, 0x00, 0x00, 0x01, 0x01,
			// column definition
			0x17, 0x00, 0x00, 0x02, 0x03, 'd', 'e', 'f', 0x00, 0x00, 0x00, 0x01, 'a', 0x00,
			0x0c, 0x3f, 0x00, 0x01, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00,
			// EOF
			0x05, 0x00, 0x00, 0x03, 0xfe, 0x00, 0x00, 0x02, 0x00,
			// row "1"
			0x02, 0x00, 0x00, 0x04, 0x01, '1',
			// ERR 1317 (query interrupted) during row delivery
			0x09, 0x00, 0x00, 0x05, 0xff, 0x25, 0x05, '#', '7', '0', '1', '0', '0',
		},
		errno:   1317,
		want:    []int64{0},
		wantIDs: []int64{0},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			conn, mc := newRWMockConn(0)
			conn.data = tc.data
			res, err := mc.Exec("UPDATE t SET a=1;INSERT INTO t VALUES (1)", nil)
			var me *MySQLError
			if !errors.As(err, &me) || me.Number != tc.errno {
				t.Fatalf("want error %d, got %v", tc.errno, err)
			}
			if res == nil {
				t.Fatal("no result with the server error")
			}
			if got := res.(Result).AllRowsAffected(); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("AllRowsAffected: got %v, want %v", got, tc.want)
			}
			if got := res.(Result).AllLastInsertIds(); !reflect.DeepEqual(got, tc.wantIDs) {
				t.Errorf("AllLastInsertIds: got %v, want %v", got, tc.wantIDs)
			}
			// The connection is still usable.
			conn.data = makePacket(1, []byte{iOK, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00})
			if _, err := mc.Exec("DO 1", nil); err != nil {
				t.Errorf("Exec after the error: %v", err)
			}
		})
	}
}

// TestExecErrorResultClosed checks that Exec keeps the Result read so far when
// the server error also closes the connection.
func TestExecErrorResultClosed(t *testing.T) {
	// OK, 1 row affected, last insert ID 5, SERVER_MORE_RESULTS_EXISTS
	okMore := func(seq byte) []byte {
		return makePacket(seq, []byte{iOK, 0x01, 0x05, 0x08, 0x00, 0x00, 0x00})
	}
	infileRequest := func(seq byte, name string) []byte {
		return makePacket(seq, append([]byte{iLocalInFile}, "Reader::"+name...))
	}
	errPacket := func(seq byte, number uint16, state string) []byte {
		return makePacket(seq, append([]byte{0xff, byte(number), byte(number >> 8), '#'}, state+"server error"...))
	}

	for _, tc := range []struct {
		name    string
		query   string
		reply   func(name string) []byte
		partial int // writes before the write that fails halfway, -1 for none
		errno   uint16
		want    []int64
		wantIDs []int64
	}{{
		name:    "command write rejected",
		query:   "INSERT INTO t VALUES (1)",
		reply:   func(string) []byte { return errPacket(1, 1153, "08S01") },
		partial: 0,
		errno:   1153,
		want:    []int64{0},
		wantIDs: []int64{0},
	}, {
		// readPacket accepts an ERR packet with a wrong sequence number, but
		// closes the connection.
		name:    "error out of sequence",
		query:   "INSERT INTO t VALUES (1)",
		reply:   func(string) []byte { return errPacket(0, 1062, "23000") },
		partial: -1,
		errno:   1062,
		want:    []int64{0},
		wantIDs: []int64{0},
	}, {
		name:    "second error out of sequence",
		query:   "INSERT INTO t VALUES (1);INSERT INTO t VALUES (2)",
		reply:   func(string) []byte { return slices.Concat(okMore(1), errPacket(0, 1062, "23000")) },
		partial: -1,
		errno:   1062,
		want:    []int64{1, 0},
		wantIDs: []int64{5, 0},
	}, {
		name:  "infile response",
		query: "INSERT INTO t VALUES (1);LOAD DATA LOCAL INFILE 'x' INTO TABLE t",
		reply: func(name string) []byte {
			// Data packet 3 and empty packet 4 are uploaded in between.
			return slices.Concat(okMore(1), infileRequest(2, name), errPacket(5, 1062, "23000"))
		},
		partial: -1,
		errno:   1062,
		want:    []int64{1, 0},
		wantIDs: []int64{5, 0},
	}, {
		name:  "infile write rejected",
		query: "INSERT INTO t VALUES (1);LOAD DATA LOCAL INFILE 'x' INTO TABLE t",
		reply: func(name string) []byte {
			return slices.Concat(okMore(1), infileRequest(2, name), errPacket(3, 1153, "08S01"))
		},
		partial: 1,
		errno:   1153,
		want:    []int64{1, 0},
		wantIDs: []int64{5, 0},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			RegisterReaderHandler(t.Name(), func() io.Reader { return strings.NewReader("1\n") })
			defer DeregisterReaderHandler(t.Name())
			var netConn net.Conn
			if tc.partial >= 0 {
				netConn = &brokenPipeConn{
					mockConn: new(mockConn),
					pending:  tc.reply(t.Name()),
					okWrites: tc.partial,
					partial:  true,
				}
			} else {
				conn := new(mockConn)
				conn.data = tc.reply(t.Name())
				netConn = conn
			}
			_, mc := newRWMockConn(0)
			mc.netConn = netConn
			mc.rawConn = netConn
			mc.maxWriteSize = defaultPacketSize

			res, err := mc.Exec(tc.query, nil)
			var me *MySQLError
			if !errors.As(err, &me) || me.Number != tc.errno {
				t.Fatalf("want error %d, got %v", tc.errno, err)
			}
			if res == nil {
				t.Fatal("no result with the server error")
			}
			if got := res.(Result).AllRowsAffected(); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("AllRowsAffected: got %v, want %v", got, tc.want)
			}
			if got := res.(Result).AllLastInsertIds(); !reflect.DeepEqual(got, tc.wantIDs) {
				t.Errorf("AllLastInsertIds: got %v, want %v", got, tc.wantIDs)
			}
			if n, err := res.RowsAffected(); n != 0 || err != nil {
				t.Errorf("RowsAffected: got %d, %v", n, err)
			}
			if id, err := res.LastInsertId(); id != 0 || err != nil {
				t.Errorf("LastInsertId: got %d, %v", id, err)
			}
			if mc.IsValid() {
				t.Error("connection is still valid")
			}
		})
	}
}

// TestExecErrorResultNetwork checks that Exec returns no Result when reading
// fails after a partial response, and that the connection is unusable.
func TestExecErrorResultNetwork(t *testing.T) {
	conn, mc := newRWMockConn(0)
	// OK, 1 row affected, SERVER_MORE_RESULTS_EXISTS, then the read fails.
	conn.data = makePacket(1, []byte{iOK, 0x01, 0x00, 0x08, 0x00, 0x00, 0x00})
	conn.maxReads = 1
	res, err := mc.Exec("UPDATE t SET a=1;UPDATE t SET a=2", nil)
	if err == nil {
		t.Fatal("no error")
	}
	var me *MySQLError
	if errors.As(err, &me) {
		t.Fatalf("got server error %v, want a network error", err)
	}
	if res != nil {
		t.Errorf("got result %v with a network error", res)
	}
	if mc.IsValid() {
		t.Error("connection is still valid")
	}
}

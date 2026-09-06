// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2016 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"bytes"
	"database/sql/driver"
	"errors"
	"net"
	"testing"
	"time"
)

var (
	errConnClosed        = errors.New("connection is closed")
	errConnTooManyReads  = errors.New("too many reads")
	errConnTooManyWrites = errors.New("too many writes")
)

// struct to mock a net.Conn for testing purposes
type mockConn struct {
	laddr         net.Addr
	raddr         net.Addr
	data          []byte
	written       []byte
	queuedReplies [][]byte
	closed        bool
	read          int
	reads         int
	writes        int
	maxReads      int
	maxWrites     int
	writeErrors   map[int]error
}

func (m *mockConn) Read(b []byte) (n int, err error) {
	if m.closed {
		return 0, errConnClosed
	}

	m.reads++
	if m.maxReads > 0 && m.reads > m.maxReads {
		return 0, errConnTooManyReads
	}

	n = copy(b, m.data)
	m.read += n
	m.data = m.data[n:]
	return
}
func (m *mockConn) Write(b []byte) (n int, err error) {
	if m.closed {
		return 0, errConnClosed
	}

	m.writes++
	if err := m.writeErrors[m.writes]; err != nil {
		return 0, err
	}
	if m.maxWrites > 0 && m.writes > m.maxWrites {
		return 0, errConnTooManyWrites
	}

	n = len(b)
	m.written = append(m.written, b...)

	if n > 0 && len(m.queuedReplies) > 0 {
		m.data = m.queuedReplies[0]
		m.queuedReplies = m.queuedReplies[1:]
	}
	return
}
func (m *mockConn) Close() error {
	m.closed = true
	return nil
}
func (m *mockConn) LocalAddr() net.Addr {
	return m.laddr
}
func (m *mockConn) RemoteAddr() net.Addr {
	return m.raddr
}
func (m *mockConn) SetDeadline(t time.Time) error {
	return nil
}
func (m *mockConn) SetReadDeadline(t time.Time) error {
	return nil
}
func (m *mockConn) SetWriteDeadline(t time.Time) error {
	return nil
}

// make sure mockConn implements the net.Conn interface
var _ net.Conn = new(mockConn)

func newRWMockConn(sequence uint8) (*mockConn, *mysqlConn) {
	conn := new(mockConn)
	cfg := NewConfig()
	if err := cfg.normalize(); err != nil {
		panic(err)
	}
	mc := &mysqlConn{
		buf:              newBuffer(),
		cfg:              cfg,
		netConn:          conn,
		closech:          make(chan struct{}),
		maxAllowedPacket: defaultMaxAllowedPacket,
		sequence:         sequence,
	}
	return conn, mc
}

func TestWriteExecutePacketValidatesArgsBeforeWriting(t *testing.T) {
	conn, mc := newRWMockConn(42)
	mc.maxAllowedPacket = 128
	stmt := mysqlStmt{mc: mc, id: 1, paramCount: 2}

	err := stmt.writeExecutePacket([]driver.Value{
		bytes.Repeat([]byte{'a'}, 64), // 64 'a' bytes, large enough to use long data.
		time.Date(10000, time.January, 1, 0, 0, 0, 0, time.UTC),
	})
	if err == nil {
		t.Fatal("expected invalid time error")
	}
	if got, want := err.Error(), "year is not in the range [1, 9999]: 10000"; got != want {
		t.Fatalf("unexpected error: got %q, want %q", got, want)
	}
	if conn.writes != 0 {
		t.Fatalf("unexpected writes before all arguments were validated: %d", conn.writes)
	}
	if mc.sequence != 42 {
		t.Fatalf("packet sequence changed before all arguments were validated: got %d, want 42", mc.sequence)
	}
}

func TestWriteExecutePacketValidatesSizeBeforeWriting(t *testing.T) {
	conn, mc := newRWMockConn(42)
	mc.maxAllowedPacket = 16
	stmt := mysqlStmt{mc: mc, id: 1, paramCount: 2}

	err := stmt.writeExecutePacket([]driver.Value{
		bytes.Repeat([]byte{'a'}, 64), // 64 'a' bytes, large enough to use long data.
		int64(1),
	})
	if err != ErrPktTooLarge {
		t.Fatalf("unexpected error: got %v, want %v", err, ErrPktTooLarge)
	}
	if conn.writes != 0 {
		t.Fatalf("unexpected writes before packet size was validated: %d", conn.writes)
	}
	if mc.sequence != 42 {
		t.Fatalf("packet sequence changed before packet size was validated: got %d, want 42", mc.sequence)
	}
}

func TestWriteExecutePacketSendsLongDataBeforeExecute(t *testing.T) {
	conn, mc := newRWMockConn(42)
	mc.maxAllowedPacket = 128
	stmt := mysqlStmt{mc: mc, id: 1, paramCount: 2}

	err := stmt.writeExecutePacket([]driver.Value{
		bytes.Repeat([]byte{'a'}, 64),         // 64 'a' bytes as a []byte long-data parameter.
		string(bytes.Repeat([]byte{'b'}, 64)), // 64 'b' bytes as a string long-data parameter.
	})
	if err != nil {
		t.Fatal(err)
	}

	var commands []byte
	for written := conn.written; len(written) > 0; {
		packetLen := 4 + getUint24(written)
		if packetLen > len(written) {
			t.Fatalf("invalid packet length: got %d, remaining bytes %d", packetLen, len(written))
		}
		commands = append(commands, written[4]) // The command is the first byte after the packet header.
		written = written[packetLen:]
	}
	if want := []byte{comStmtSendLongData, comStmtSendLongData, comStmtExecute}; !bytes.Equal(commands, want) {
		t.Fatalf("unexpected command order: got %v, want %v", commands, want)
	}
}

func TestWriteExecutePacketResetsStmtAfterLongDataError(t *testing.T) {
	conn, mc := newRWMockConn(42)
	mc.maxAllowedPacket = 128
	stmt := mysqlStmt{mc: mc, id: 1, paramCount: 2}
	conn.writeErrors = map[int]error{2: errors.New("long data write failed")}
	conn.queuedReplies = [][]byte{{
		0x07, 0x00, 0x00, 0x01, // Packet header: 7-byte payload, sequence 1.
		iOK,        // OK packet header.
		0x00,       // Zero affected rows.
		0x00,       // Zero last insert ID.
		0x02, 0x00, // SERVER_STATUS_AUTOCOMMIT.
		0x00, 0x00, // Zero warnings.
	}}

	err := stmt.writeExecutePacket([]driver.Value{
		bytes.Repeat([]byte{'a'}, 64), // 64 'a' bytes sent successfully as long data.
		bytes.Repeat([]byte{'b'}, 64), // 64 'b' bytes whose long-data write fails.
	})
	if err != errBadConnNoWrite {
		t.Fatalf("unexpected error: got %v, want %v", err, errBadConnNoWrite)
	}

	var commands []byte
	for written := conn.written; len(written) > 0; {
		packetLen := 4 + getUint24(written)
		if packetLen > len(written) {
			t.Fatalf("invalid packet length: got %d, remaining bytes %d", packetLen, len(written))
		}
		commands = append(commands, written[4]) // The command is the first byte after the packet header.
		written = written[packetLen:]
	}
	if want := []byte{comStmtSendLongData, comStmtReset}; !bytes.Equal(commands, want) {
		t.Fatalf("unexpected command order: got %v, want %v", commands, want)
	}
	if len(conn.data) != 0 {
		t.Fatal("COM_STMT_RESET response was not consumed")
	}
}

func TestReadPacketSingleByte(t *testing.T) {
	conn := new(mockConn)
	mc := &mysqlConn{
		netConn: conn,
		buf:     newBuffer(),
		cfg:     NewConfig(),
	}

	conn.data = []byte{0x01, 0x00, 0x00, 0x00, 0xff}
	conn.maxReads = 1
	packet, err := mc.readPacket()
	if err != nil {
		t.Fatal(err)
	}
	if len(packet) != 1 {
		t.Fatalf("unexpected packet length: expected %d, got %d", 1, len(packet))
	}
	if packet[0] != 0xff {
		t.Fatalf("unexpected packet content: expected %x, got %x", 0xff, packet[0])
	}
}

func TestReadPacketWrongSequenceID(t *testing.T) {
	for _, testCase := range []struct {
		ClientSequenceID byte
		ServerSequenceID byte
		ExpectedErr      error
	}{
		{
			ClientSequenceID: 1,
			ServerSequenceID: 0,
			ExpectedErr:      ErrPktSync,
		},
		{
			ClientSequenceID: 0,
			ServerSequenceID: 0x42,
			ExpectedErr:      ErrPktSync,
		},
	} {
		conn, mc := newRWMockConn(testCase.ClientSequenceID)

		conn.data = []byte{0x01, 0x00, 0x00, testCase.ServerSequenceID, 0x22}
		_, err := mc.readPacket()
		if err != testCase.ExpectedErr {
			t.Errorf("expected %v, got %v", testCase.ExpectedErr, err)
		}

		// connection should not be returned to the pool in this state
		if mc.IsValid() {
			t.Errorf("expected IsValid() to be false")
		}
	}
}

func TestReadPacketSplit(t *testing.T) {
	conn := new(mockConn)
	mc := &mysqlConn{
		netConn: conn,
		buf:     newBuffer(),
		cfg:     NewConfig(),
	}

	data := make([]byte, maxPacketSize*2+4*3)
	const pkt2ofs = maxPacketSize + 4
	const pkt3ofs = 2 * (maxPacketSize + 4)

	// case 1: payload has length maxPacketSize
	data = data[:pkt2ofs+4]

	// 1st packet has maxPacketSize length and sequence id 0
	// ff ff ff 00 ...
	data[0] = 0xff
	data[1] = 0xff
	data[2] = 0xff

	// mark the payload start and end of 1st packet so that we can check if the
	// content was correctly appended
	data[4] = 0x11
	data[maxPacketSize+3] = 0x22

	// 2nd packet has payload length 0 and sequence id 1
	// 00 00 00 01
	data[pkt2ofs+3] = 0x01

	conn.data = data
	conn.maxReads = 3
	packet, err := mc.readPacket()
	if err != nil {
		t.Fatal(err)
	}
	if len(packet) != maxPacketSize {
		t.Fatalf("unexpected packet length: expected %d, got %d", maxPacketSize, len(packet))
	}
	if packet[0] != 0x11 {
		t.Fatalf("unexpected payload start: expected %x, got %x", 0x11, packet[0])
	}
	if packet[maxPacketSize-1] != 0x22 {
		t.Fatalf("unexpected payload end: expected %x, got %x", 0x22, packet[maxPacketSize-1])
	}

	// case 2: payload has length which is a multiple of maxPacketSize
	data = data[:cap(data)]

	// 2nd packet now has maxPacketSize length
	data[pkt2ofs] = 0xff
	data[pkt2ofs+1] = 0xff
	data[pkt2ofs+2] = 0xff

	// mark the payload start and end of the 2nd packet
	data[pkt2ofs+4] = 0x33
	data[pkt2ofs+maxPacketSize+3] = 0x44

	// 3rd packet has payload length 0 and sequence id 2
	// 00 00 00 02
	data[pkt3ofs+3] = 0x02

	conn.data = data
	conn.reads = 0
	conn.maxReads = 5
	mc.sequence = 0
	packet, err = mc.readPacket()
	if err != nil {
		t.Fatal(err)
	}
	if len(packet) != 2*maxPacketSize {
		t.Fatalf("unexpected packet length: expected %d, got %d", 2*maxPacketSize, len(packet))
	}
	if packet[0] != 0x11 {
		t.Fatalf("unexpected payload start: expected %x, got %x", 0x11, packet[0])
	}
	if packet[2*maxPacketSize-1] != 0x44 {
		t.Fatalf("unexpected payload end: expected %x, got %x", 0x44, packet[2*maxPacketSize-1])
	}

	// case 3: payload has a length larger maxPacketSize, which is not an exact
	// multiple of it
	data = data[:pkt2ofs+4+42]
	data[pkt2ofs] = 0x2a
	data[pkt2ofs+1] = 0x00
	data[pkt2ofs+2] = 0x00
	data[pkt2ofs+4+41] = 0x44

	conn.data = data
	conn.reads = 0
	conn.maxReads = 4
	mc.sequence = 0
	packet, err = mc.readPacket()
	if err != nil {
		t.Fatal(err)
	}
	if len(packet) != maxPacketSize+42 {
		t.Fatalf("unexpected packet length: expected %d, got %d", maxPacketSize+42, len(packet))
	}
	if packet[0] != 0x11 {
		t.Fatalf("unexpected payload start: expected %x, got %x", 0x11, packet[0])
	}
	if packet[maxPacketSize+41] != 0x44 {
		t.Fatalf("unexpected payload end: expected %x, got %x", 0x44, packet[maxPacketSize+41])
	}
}

func TestReadPacketFail(t *testing.T) {
	conn := new(mockConn)
	mc := &mysqlConn{
		netConn: conn,
		buf:     newBuffer(),
		closech: make(chan struct{}),
		cfg:     NewConfig(),
	}

	// illegal empty (stand-alone) packet
	conn.data = []byte{0x00, 0x00, 0x00, 0x00}
	conn.maxReads = 1
	_, err := mc.readPacket()
	if err != ErrInvalidConn {
		t.Errorf("expected ErrInvalidConn, got %v", err)
	}

	// reset
	conn.reads = 0
	mc.sequence = 0
	mc.buf = newBuffer()

	// fail to read header
	conn.closed = true
	_, err = mc.readPacket()
	if err != ErrInvalidConn {
		t.Errorf("expected ErrInvalidConn, got %v", err)
	}

	// reset
	conn.closed = false
	conn.reads = 0
	mc.sequence = 0
	mc.buf = newBuffer()

	// fail to read body
	conn.maxReads = 1
	_, err = mc.readPacket()
	if err != ErrInvalidConn {
		t.Errorf("expected ErrInvalidConn, got %v", err)
	}
}

// https://github.com/go-sql-driver/mysql/pull/801
// not-NUL terminated plugin_name in init packet
func TestRegression801(t *testing.T) {
	conn := new(mockConn)
	mc := &mysqlConn{
		netConn:  conn,
		buf:      newBuffer(),
		cfg:      new(Config),
		sequence: 42,
		closech:  make(chan struct{}),
	}

	conn.data = []byte{72, 0, 0, 42, 10, 53, 46, 53, 46, 56, 0, 165, 0, 0, 0,
		60, 70, 63, 58, 68, 104, 34, 97, 0, 223, 247, 33, 2, 0, 15, 128, 21, 0,
		0, 0, 0, 0, 0, 0, 0, 0, 0, 98, 120, 114, 47, 85, 75, 109, 99, 51, 77,
		50, 64, 0, 109, 121, 115, 113, 108, 95, 110, 97, 116, 105, 118, 101, 95,
		112, 97, 115, 115, 119, 111, 114, 100}
	conn.maxReads = 1

	authData, serverCapabilities, serverExtendedCapabilities, pluginName, err := mc.readHandshakePacket()
	if err != nil {
		t.Fatalf("got error: %v", err)
	}

	if serverCapabilities != 2148530143 {
		t.Fatalf("expected serverCapabilities to be 2148530143, got %v", serverCapabilities)
	}

	if serverExtendedCapabilities != 0 {
		t.Fatalf("expected serverExtendedCapabilities to be 0, got %v", serverExtendedCapabilities)
	}

	if pluginName != "mysql_native_password" {
		t.Errorf("expected plugin name 'mysql_native_password', got '%s'", pluginName)
	}

	expectedAuthData := []byte{60, 70, 63, 58, 68, 104, 34, 97, 98, 120, 114,
		47, 85, 75, 109, 99, 51, 77, 50, 64}
	if !bytes.Equal(authData, expectedAuthData) {
		t.Errorf("expected authData '%v', got '%v'", expectedAuthData, authData)
	}
}

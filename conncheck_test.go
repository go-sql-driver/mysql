// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2013 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || solaris || illumos

package mysql

import (
	"bytes"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
	"testing"
	"time"
)

func TestStaleConnectionChecks(t *testing.T) {
	runTestsParallel(t, dsn, func(dbt *DBTest, _ string) {
		dbt.mustExec("SET @@SESSION.wait_timeout = 2")

		if err := dbt.db.Ping(); err != nil {
			dbt.Fatal(err)
		}

		// wait for MySQL to close our connection
		time.Sleep(3 * time.Second)

		tx, err := dbt.db.Begin()
		if err != nil {
			dbt.Fatal(err)
		}

		if err := tx.Rollback(); err != nil {
			dbt.Fatal(err)
		}
	})
}

// connCheck must not consume bytes needed by the TLS and compression readers.
func TestConnCheckTCP(t *testing.T) {
	for _, tc := range []struct {
		name  string
		data  bool
		close bool
	}{
		{"idle", false, false},
		{"data", true, false},
		{"data-and-close", true, true},
		{"close", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			client, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			server, err := listener.Accept()
			if err != nil {
				t.Fatal(err)
			}
			defer server.Close()
			deadline := time.Now().Add(5 * time.Second)
			client.SetDeadline(deadline)
			server.SetDeadline(deadline)
			payload := []byte("hello")
			if tc.data {
				if _, err := server.Write(payload); err != nil {
					t.Fatal(err)
				}
			}
			if tc.close {
				server.Close()
			}
			want := error(nil)
			if tc.data {
				want = errUnexpectedRead
			} else if tc.close {
				want = io.EOF
			}
			// A stalled check is interrupted even on platforms where socket
			// deadlines do not apply to the raw callback.
			timer := time.AfterFunc(5*time.Second, func() { client.Close() })
			defer timer.Stop()
			for {
				err := connCheck(client)
				if err == want {
					break
				}
				if err != nil || time.Now().After(deadline) {
					t.Fatalf("connCheck = %v, want %v", err, want)
				}
				time.Sleep(time.Millisecond)
			}
			if tc.data {
				if err := connCheck(client); err != errUnexpectedRead {
					t.Fatalf("second check = %v", err)
				}
				got := make([]byte, len(payload))
				if _, err := io.ReadFull(client, got); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(got, payload) {
					t.Fatalf("read %q, want %q", got, payload)
				}
				if tc.close {
					if n, err := client.Read(got); n != 0 || err != io.EOF {
						t.Fatalf("after data: n=%d, err=%v", n, err)
					}
				}
			}
		})
	}
}

func TestConnCheckNoSyscallConn(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	if err := connCheck(client); err != nil {
		t.Fatal(err)
	}
}

// pipeConn models a custom dialer backed by a subprocess's stdin/stdout.
// Unlike net.Pipe, os.Pipe exposes a real, non-socket syscall.Conn.
type pipeConn struct {
	*os.File
	writer *os.File
}

func (c *pipeConn) Write(p []byte) (int, error)        { return c.writer.Write(p) }
func (c *pipeConn) Close() error                       { return errors.Join(c.File.Close(), c.writer.Close()) }
func (c *pipeConn) LocalAddr() net.Addr                { return &net.UnixAddr{Name: "pipe", Net: "pipe"} }
func (c *pipeConn) RemoteAddr() net.Addr               { return c.LocalAddr() }
func (c *pipeConn) SetWriteDeadline(t time.Time) error { return c.writer.SetWriteDeadline(t) }
func (c *pipeConn) SetDeadline(t time.Time) error {
	return errors.Join(c.SetReadDeadline(t), c.SetWriteDeadline(t))
}

func newPipeTestConn(t *testing.T) (*mysqlConn, *os.File, *os.File) {
	t.Helper()
	reader, peerWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reader.Close(); peerWriter.Close() })
	peerReader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { peerReader.Close(); writer.Close() })
	conn := &pipeConn{File: reader, writer: writer}
	var _ net.Conn = conn
	var _ syscall.Conn = conn
	deadline := time.Now().Add(5 * time.Second)
	for _, f := range []*os.File{reader, peerWriter, peerReader, writer} {
		if err := f.SetDeadline(deadline); err != nil {
			t.Fatal(err)
		}
	}
	cfg := NewConfig()
	cfg.Logger = &NopLogger{}
	cfg.ReadTimeout = time.Second
	mc := &mysqlConn{netConn: conn, rawConn: conn, cfg: cfg, buf: newBuffer(), closech: make(chan struct{}), maxAllowedPacket: maxPacketSize}
	t.Cleanup(mc.close)
	return mc, peerReader, peerWriter
}

func TestConnCheckPipe(t *testing.T) {
	mc, peerReader, peerWriter := newPipeTestConn(t)
	conn := mc.netConn
	check := func() {
		t.Helper()
		for range 2 {
			if err := connCheck(conn); err != nil {
				t.Errorf("connCheck(pipe) = %v, want nil", err)
			}
			if err := mc.ResetSession(t.Context()); err != nil {
				t.Errorf("ResetSession(pipe) = %v, want nil", err)
			}
		}
		if mc.closed.Load() {
			t.Fatal("connection was closed")
		}
	}
	check() // An idle pipe must not be rejected on reuse.
	payload := []byte("server response")
	if _, err := peerWriter.Write(payload); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("read %q, want %q", got, payload)
	}
	if _, err := conn.Write(payload); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(peerReader, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("peer read %q, want %q", got, payload)
	}
	check()
	// A genuinely unusable descriptor must still fail the check.
	if err := conn.(*pipeConn).File.Close(); err != nil {
		t.Fatal(err)
	}
	if err := connCheck(conn); err == nil {
		t.Error("connCheck(closed pipe) = nil")
	}
	if err := mc.ResetSession(t.Context()); err != driver.ErrBadConn {
		t.Errorf("ResetSession(closed pipe) = %v, want ErrBadConn", err)
	}
}

func TestConnCheckPipePeerEOF(t *testing.T) {
	mc, peerReader, peerWriter := newPipeTestConn(t)
	// Leave the peer's input open: a query write could otherwise succeed.
	if err := peerWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := connCheck(mc.netConn); err != io.EOF {
		t.Errorf("connCheck(half-closed pipe) = %v, want EOF", err)
	}
	resetErr := mc.ResetSession(t.Context())
	if resetErr == nil {
		// Reproduce the lost safe retry: writing succeeds, then reading fails.
		_, err := mc.Exec("SELECT 1", nil)
		t.Errorf("ResetSession accepted half-closed pipe; Exec returned %v", err)
	}
	if resetErr != driver.ErrBadConn {
		t.Errorf("ResetSession(half-closed pipe) = %v, want ErrBadConn", resetErr)
	}
	mc.close()
	sent, err := io.ReadAll(peerReader)
	if err != nil {
		t.Fatal(err)
	}
	if len(sent) != 0 {
		t.Errorf("sent %d bytes before rejecting half-closed pipe", len(sent))
	}
}

func TestConnCheckPipePendingData(t *testing.T) {
	for _, reset := range []bool{false, true} {
		t.Run(fmt.Sprint("ResetSession=", reset), func(t *testing.T) {
			mc, _, peerWriter := newPipeTestConn(t)
			payload := []byte("unsolicited data")
			if _, err := peerWriter.Write(payload); err != nil {
				t.Fatal(err)
			}
			if reset {
				if err := mc.ResetSession(t.Context()); err != driver.ErrBadConn {
					t.Fatalf("ResetSession = %v, want ErrBadConn", err)
				}
			} else {
				if err := connCheck(mc.netConn); err != errUnexpectedReadNonSocket {
					t.Fatalf("connCheck = %v, want non-peek unexpected-data error", err)
				}
			}
			// The old pipe check consumes one byte before rejecting the
			// connection. Do not run the peek-only diagnostic on its remainder.
			got := make([]byte, len(payload)-1)
			if _, err := io.ReadFull(mc.netConn, got); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, payload[1:]) {
				t.Fatalf("remaining data = %q", got)
			}
		})
	}
}

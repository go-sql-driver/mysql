// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2026 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing"
)

type infileTestReader struct {
	io.Reader
	readErr, closeErr error
	closes            int
}

func (r *infileTestReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if r.readErr != nil {
		return n, r.readErr
	}
	return n, err
}

func (r *infileTestReader) Close() error {
	r.closes++
	return r.closeErr
}

// Wrap a protocol packet in an actual zlib frame when compression is enabled.
func infileTestPacket(t *testing.T, compressed bool, seq byte, body []byte) []byte {
	t.Helper()
	packet := makePacket(seq, body)
	if !compressed {
		return packet
	}
	var buf bytes.Buffer
	if err := zCompress(packet, &buf); err != nil {
		t.Fatal(err)
	}
	header := make([]byte, 7)
	putUint24(header, buf.Len())
	header[3] = seq
	putUint24(header[4:], len(packet))
	return append(header, buf.Bytes()...)
}

func TestInfileFailureClosesConnection(t *testing.T) {
	readErr := errors.New("upload aborted")
	closeErr := errors.New("upload close failed")
	for _, compressed := range []bool{false, true} {
		for _, tc := range []struct {
			name, data        string
			readErr, closeErr error
			want              string
		}{
			{name: "success", data: "1,x\n"},
			{name: "partial read", data: "1,x\n", readErr: readErr, want: readErr.Error()},
			{name: "empty read", readErr: readErr, want: readErr.Error()},
			{name: "close", data: "1,x\n", closeErr: closeErr, want: closeErr.Error()},
			{name: "read and close", data: "1,x\n", readErr: readErr, closeErr: closeErr, want: readErr.Error()},
			{name: "unregistered reader", want: "is not registered"},
			{name: "nil reader", want: "is <nil>"},
			{name: "unregistered file", want: "is not registered"},
			{name: "missing file", want: "open error"},
		} {
			t.Run(fmt.Sprintf("compress=%t/%s", compressed, tc.name), func(t *testing.T) {
				conn, mc := newRWMockConn(2) // The LOCAL INFILE request was sequence 1.
				mc.rawConn = conn
				mc.maxWriteSize = defaultPacketSize
				mc.compress = compressed
				if compressed {
					mc.compIO = newCompIO(mc)
					mc.compressSequence = 2
				}
				reader := &infileTestReader{Reader: strings.NewReader(tc.data), readErr: tc.readErr, closeErr: tc.closeErr}
				name := t.Name()
				request := "Reader::" + name
				wantCloses := 1
				switch tc.name {
				case "unregistered file", "missing file":
					request = filepath.ToSlash(filepath.Join(t.TempDir(), "missing.csv"))
					wantCloses = 0
					if tc.name == "missing file" {
						RegisterLocalFile(request)
						t.Cleanup(func() { DeregisterLocalFile(request) })
					}
				case "unregistered reader":
					wantCloses = 0
				case "nil reader":
					RegisterReaderHandler(name, func() io.Reader { return nil })
					t.Cleanup(func() { DeregisterReaderHandler(name) })
					wantCloses = 0
				default:
					RegisterReaderHandler(name, func() io.Reader { return reader })
					t.Cleanup(func() { DeregisterReaderHandler(name) })
				}
				seq := byte(3) // Empty terminating packet, plus an optional data packet.
				if tc.data != "" {
					seq++
				}
				status := statusInAutocommit | statusMoreResultsExists
				conn.data = infileTestPacket(t, compressed, seq, []byte{iOK, 0, 0, byte(status), 0, 0, 0})
				// No trailing results are available yet. Socket/buffer liveness checks
				// cannot detect a delayed result from the rest of the batch.
				err := mc.clearResult().handleInFileRequest(request)
				if tc.name == "missing file" {
					// The OS-specific error text is not part of the contract.
					if err == nil {
						t.Fatal("expected an open error")
					}
				} else if tc.want == "" {
					if err != nil {
						t.Fatal(err)
					}
				} else if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("error = %v, want %q", err, tc.want)
				}
				if tc.readErr != nil && !errors.Is(err, tc.readErr) {
					t.Fatalf("read error lost: %v", err)
				}
				if mc.status != status {
					t.Errorf("server status = %x, want %x", mc.status, status)
				}
				if reader.closes != wantCloses {
					t.Errorf("Close called %d times, want %d", reader.closes, wantCloses)
				}
				if tc.want == "" {
					if mc.closed.Load() {
						t.Fatal("successful upload closed the connection")
					}
					return
				}
				if mc.IsValid() || !conn.closed {
					t.Error("failed upload left the connection reusable")
				}
				if err := mc.ResetSession(context.Background()); err != driver.ErrBadConn {
					t.Errorf("ResetSession = %v, want ErrBadConn", err)
				}
			})
		}
	}
}

func TestInfileWriteFailureNotRetryable(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		for _, write := range []int{1, 2} { // Data packet and terminating packet.
			t.Run(fmt.Sprintf("compress=%t/write=%d", compressed, write), func(t *testing.T) {
				conn, mc := newRWMockConn(2)
				mc.rawConn = conn
				mc.maxWriteSize = defaultPacketSize
				mc.compress = compressed
				if compressed {
					mc.compIO = newCompIO(mc)
					mc.compressSequence = 2
				}
				conn.writeErrors = map[int]error{write: errors.New("upload write failed")}
				reader := &infileTestReader{Reader: strings.NewReader("1,x\n")}
				RegisterReaderHandler(t.Name(), func() io.Reader { return reader })
				defer DeregisterReaderHandler(t.Name())
				err := mc.clearResult().handleInFileRequest("Reader::" + t.Name())
				if err == nil || mc.markBadConn(err) == driver.ErrBadConn {
					t.Errorf("error = %v; an already executed batch must not be retried", err)
				}
				if mc.IsValid() || !conn.closed || reader.closes != 1 {
					t.Errorf("failed upload not cleaned up: valid=%t closed=%t closes=%d", mc.IsValid(), conn.closed, reader.closes)
				}
			})
		}
	}
}

func TestInfileFailurePoolIsolation(t *testing.T) {
	if !available {
		t.Skipf("MySQL server not running on %s", netAddr)
	}
	for _, compressed := range []bool{false, true} {
		for _, operation := range []string{"Exec", "Query", "NextResultSet", "RowsClose"} {
			for _, failure := range []string{"read", "close", "unregistered file", "unregistered reader"} {
				t.Run(fmt.Sprintf("compress=%t/%s/%s", compressed, operation, failure), func(t *testing.T) {
					// Disable liveness checking so the test also exercises the Windows
					// behavior and cannot pass just because pending bytes were noticed.
					db, err := sql.Open(driverNameTest, dsn+fmt.Sprintf("&multiStatements=true&compress=%t&checkConnLiveness=false", compressed))
					if err != nil {
						t.Fatal(err)
					}
					defer db.Close()
					db.SetMaxOpenConns(1)
					db.SetMaxIdleConns(1)
					const table = "test_infile_pool_isolation"
					if _, err := db.Exec("DROP TABLE IF EXISTS " + table + "; CREATE TABLE " + table + " (id INT, value TEXT)"); err != nil {
						t.Fatal(err)
					}
					defer db.Exec("DROP TABLE IF EXISTS " + table)
					var before int64
					if err := db.QueryRow("SELECT CONNECTION_ID()").Scan(&before); err != nil {
						t.Fatal(err)
					}
					clientErr := errors.New("client upload " + failure + " failed")
					reader := &infileTestReader{Reader: strings.NewReader("1,x\n")}
					request, want := "Reader::"+t.Name(), clientErr.Error()
					calls := 0
					switch failure {
					case "read", "close":
						if failure == "read" {
							reader.readErr = clientErr
						} else {
							reader.closeErr = clientErr
						}
						RegisterReaderHandler(t.Name(), func() io.Reader { calls++; return reader })
						defer DeregisterReaderHandler(t.Name())
					case "unregistered file":
						request, want = "unregistered-infile.csv", "is not registered"
					case "unregistered reader":
						want = "is not registered"
					}
					batch := fmt.Sprintf("LOAD DATA LOCAL INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY ','; SELECT 'tenant-A-secret', CONNECTION_ID()", request, table)
					switch operation {
					case "Exec":
						_, err = db.Exec(batch)
					case "Query":
						var rows *sql.Rows
						rows, err = db.Query(batch)
						if rows != nil {
							rows.Close()
						}
					default:
						var rows *sql.Rows
						rows, err = db.Query("SELECT 0; " + batch)
						if err != nil {
							t.Fatal(err)
						}
						if operation == "RowsClose" {
							err = rows.Close()
						} else {
							if rows.NextResultSet() {
								t.Error("unexpected next result set after failed upload")
							}
							err = rows.Err()
							rows.Close()
						}
					}
					if err == nil || !strings.Contains(err.Error(), want) {
						t.Errorf("batch error = %v, want %q", err, want)
					}
					if failure == "read" || failure == "close" {
						if !errors.Is(err, clientErr) || calls != 1 || reader.closes != 1 {
							t.Errorf("upload error/retry/cleanup: err=%v calls=%d closes=%d", err, calls, reader.closes)
						}
					}
					if idle := db.Stats().Idle; idle != 0 {
						t.Errorf("failed connection returned to pool: %d idle", idle)
					}
					var got string
					var after int64
					if err := db.QueryRow("SELECT 'tenant-B-row', CONNECTION_ID()").Scan(&got, &after); err != nil {
						t.Fatal(err)
					}
					if got != "tenant-B-row" || before == after {
						t.Errorf("next caller got %q on connection %d (failed connection %d)", got, after, before)
					}
				})
			}
		}
	}
}

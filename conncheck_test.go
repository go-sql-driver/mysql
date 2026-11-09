// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2013 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || solaris || illumos || windows

package mysql

import (
	"bytes"
	"io"
	"net"
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

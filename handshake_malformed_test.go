// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2026 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

// serveHandshake accepts one connection, writes each payload as a MySQL
// packet (sequence 0, 2, ...), and keeps the socket open briefly.
func serveHandshake(t *testing.T, packets ...[]byte) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(3 * time.Second))
		for i, p := range packets {
			h := []byte{byte(len(p)), byte(len(p) >> 8), byte(len(p) >> 16), byte(i * 2)}
			if _, err := c.Write(append(h, p...)); err != nil {
				return
			}
			if i+1 < len(packets) {
				buf := make([]byte, 4096)
				_, _ = c.Read(buf)
			}
		}
		time.Sleep(500 * time.Millisecond)
	}()
	return ln.Addr().String()
}

func connectHandshake(t *testing.T, addr string) (err error, panicked any) {
	t.Helper()
	defer func() { panicked = recover() }()
	cfg := NewConfig()
	cfg.Net, cfg.Addr, cfg.User, cfg.Passwd = "tcp", addr, "u", "p"
	cfg.Timeout = 2 * time.Second
	conn, err := NewConnector(cfg)
	if err != nil {
		return err, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := conn.Connect(ctx)
	if err == nil {
		_ = c.Close()
	}
	return err, nil
}

func TestShortInitialHandshakeReturnsError(t *testing.T) {
	addr := serveHandshake(t, []byte{0x0a, 0x00})
	err, panicked := connectHandshake(t, addr)
	if panicked != nil {
		t.Fatalf("PANIC (expected ErrMalformPkt): %v", panicked)
	}
	if !errors.Is(err, ErrMalformPkt) {
		t.Fatalf("expected ErrMalformPkt, got %v", err)
	}
}

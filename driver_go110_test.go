// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2018 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

// +build go1.10

package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net"
	"testing"
	"time"
)

var _ driver.DriverContext = &MySQLDriver{}

type dialCtxKey struct{}

func TestConnectorObeysDialTimeouts(t *testing.T) {
	if !available {
		t.Skipf("MySQL server not running on %s", netAddr)
	}

	RegisterDialContext("dialctxtest", func(ctx context.Context, addr string) (net.Conn, error) {
		var d net.Dialer
		if !ctx.Value(dialCtxKey{}).(bool) {
			return nil, fmt.Errorf("test error: query context is not propagated to our dialer")
		}
		return d.DialContext(ctx, prot, addr)
	})

	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@dialctxtest(%s)/%s?timeout=30s", user, pass, addr, dbname))
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}
	defer db.Close()

	ctx := context.WithValue(context.Background(), dialCtxKey{}, true)

	_, err = db.ExecContext(ctx, "DO 1")
	if err != nil {
		t.Fatal(err)
	}
}

func configForTests(t *testing.T) *Config {
	if !available {
		t.Skipf("MySQL server not running on %s", netAddr)
	}

	mycnf := NewConfig()
	mycnf.User = user
	mycnf.Passwd = pass
	mycnf.Addr = addr
	mycnf.Net = prot
	mycnf.DBName = dbname
	return mycnf
}

func TestNewConnector(t *testing.T) {
	mycnf := configForTests(t)
	conn, err := NewConnector(mycnf)
	if err != nil {
		t.Fatal(err)
	}

	db := sql.OpenDB(conn)
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
}

type slowConnection struct {
	net.Conn
	slowdown time.Duration
}

func (sc *slowConnection) Read(b []byte) (int, error) {
	time.Sleep(sc.slowdown)
	return sc.Conn.Read(b)
}

type connectorHijack struct {
	driver.Connector
	connErr error
}

func (cw *connectorHijack) Connect(ctx context.Context) (driver.Conn, error) {
	var conn driver.Conn
	conn, cw.connErr = cw.Connector.Connect(ctx)
	return conn, cw.connErr
}

func TestConnectorTimeoutsDuringOpen(t *testing.T) {
	RegisterDialContext("slowconn", func(ctx context.Context, addr string) (net.Conn, error) {
		var d net.Dialer
		conn, err := d.DialContext(ctx, prot, addr)
		if err != nil {
			return nil, err
		}
		return &slowConnection{Conn: conn, slowdown: 100 * time.Millisecond}, nil
	})

	mycnf := configForTests(t)
	mycnf.Net = "slowconn"

	conn, err := NewConnector(mycnf)
	if err != nil {
		t.Fatal(err)
	}

	hijack := &connectorHijack{Connector: conn}

	db := sql.OpenDB(hijack)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err = db.ExecContext(ctx, "DO 1")
	if err != context.DeadlineExceeded {
		t.Fatalf("ExecContext should have timed out")
	}
	if hijack.connErr != context.DeadlineExceeded {
		t.Fatalf("(*Connector).Connect should have timed out")
	}
}

// A connection which can only be closed.
type dummyConnection struct {
	net.Conn
	closed bool
}

func (d *dummyConnection) Close() error {
	d.closed = true
	return nil
}

func TestConnectorTimeoutsWatchCancel(t *testing.T) {
	var (
		cancel  func()           // Used to cancel the context just after connecting.
		created *dummyConnection // The created connection.
	)

	RegisterDialContext("TestConnectorTimeoutsWatchCancel", func(ctx context.Context, addr string) (net.Conn, error) {
		// Canceling at this time triggers the watchCancel error branch in Connect().
		cancel()
		created = &dummyConnection{}
		return created, nil
	})

	mycnf := NewConfig()
	mycnf.User = "root"
	mycnf.Addr = "foo"
	mycnf.Net = "TestConnectorTimeoutsWatchCancel"

	conn, err := NewConnector(mycnf)
	if err != nil {
		t.Fatal(err)
	}

	db := sql.OpenDB(conn)
	defer db.Close()

	var ctx context.Context
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	if _, err := db.Conn(ctx); err != context.Canceled {
		t.Errorf("got %v, want context.Canceled", err)
	}

	if created == nil {
		t.Fatal("no connection created")
	}
	if !created.closed {
		t.Errorf("connection not closed")
	}
}

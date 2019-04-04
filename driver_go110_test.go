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
)

var _ driver.DriverContext = &MySQLDriver{}

type dialCtxKey struct{}

func TestConnectorObeysDialTimeouts(t *testing.T) {
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

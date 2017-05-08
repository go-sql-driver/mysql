// +build go1.8

// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2012 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"context"
	"database/sql/driver"
)

// Ping implements driver.Pinger interface
func (mc *mysqlConn) Ping(ctx context.Context) error {
	if mc.netConn == nil {
		errLog.Print(ErrInvalidConn)
		return driver.ErrBadConn
	}
	if err := mc.writeCommandPacket(ctx, comPing); err != nil {
		errLog.Print(err)
		return err
	}

	if _, err := mc.readResultOK(ctx); err != nil {
		errLog.Print(err)
		return err
	}
	return nil
}

// BeginTx implements driver.ConnBeginTx interface
func (mc *mysqlConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return mc.beginTx(ctx, txOptions(opts))
}

func (mc *mysqlConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return mc.prepareContext(ctx, query)
}

// QueryContext implements driver.QueryerContext interface
func (mc *mysqlConn) QueryContext(ctx context.Context, query string, args []driver.Value) (driver.Rows, error) {
	return mc.queryContext(ctx, query, args)
}

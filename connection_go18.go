// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2012 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

// +build go1.8

package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
)

type setfinish interface {
	setFinish(f func())
}

// Ping implements driver.Pinger interface
func (mc *mysqlConn) Ping(ctx context.Context) error {
	if mc.isBroken() {
		errLog.Print(ErrInvalidConn)
		return driver.ErrBadConn
	}

	if err := mc.watchCancel(ctx); err != nil {
		return err
	}
	defer mc.finish()

	if err := mc.writeCommandPacket(comPing); err != nil {
		return err
	}
	if _, err := mc.readResultOK(); err != nil {
		return err
	}

	return nil
}

// BeginTx implements driver.ConnBeginTx interface
func (mc *mysqlConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if sql.IsolationLevel(opts.Isolation) != sql.LevelDefault {
		return nil, errors.New("mysql: isolation levels not supported")
	}

	if err := mc.watchCancel(ctx); err != nil {
		return nil, err
	}

	var err error
	var tx driver.Tx
	if opts.ReadOnly {
		tx, err = mc.beginReadOnly()
	} else {
		tx, err = mc.Begin()
	}
	mc.finish()
	if err != nil {
		return nil, err
	}

	select {
	default:
	case <-ctx.Done():
		tx.Rollback()
		return nil, ctx.Err()
	}
	return tx, err
}

func (mc *mysqlConn) beginReadOnly() (driver.Tx, error) {
	if mc.isBroken() {
		errLog.Print(ErrInvalidConn)
		return nil, driver.ErrBadConn
	}
	// https://dev.mysql.com/doc/refman/5.7/en/innodb-performance-ro-txn.html
	err := mc.exec("START TRANSACTION READ ONLY")
	if err != nil {
		return nil, err
	}

	return &mysqlTx{mc}, nil
}

func (mc *mysqlConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}

	if err := mc.watchCancel(ctx); err != nil {
		return nil, err
	}

	rows, err := mc.Query(query, dargs)
	if err != nil {
		mc.finish()
		return nil, err
	}
	if set, ok := rows.(setfinish); ok {
		set.setFinish(mc.finish)
	} else {
		mc.finish()
	}
	return rows, err
}

func (mc *mysqlConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}

	if err := mc.watchCancel(ctx); err != nil {
		return nil, err
	}
	defer mc.finish()

	return mc.Exec(query, dargs)
}

func (mc *mysqlConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if err := mc.watchCancel(ctx); err != nil {
		return nil, err
	}

	stmt, err := mc.Prepare(query)
	mc.finish()
	if err != nil {
		return nil, err
	}

	select {
	default:
	case <-ctx.Done():
		stmt.Close()
		return nil, ctx.Err()
	}
	return stmt, nil
}

func (stmt *mysqlStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}

	if err := stmt.mc.watchCancel(ctx); err != nil {
		return nil, err
	}

	rows, err := stmt.Query(dargs)
	if err != nil {
		stmt.mc.finish()
		return nil, err
	}
	if set, ok := rows.(setfinish); ok {
		set.setFinish(stmt.mc.finish)
	} else {
		stmt.mc.finish()
	}
	return rows, err
}

func (stmt *mysqlStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}

	if err := stmt.mc.watchCancel(ctx); err != nil {
		return nil, err
	}
	defer stmt.mc.finish()

	return stmt.Exec(dargs)
}

func (mc *mysqlConn) watchCancel(ctx context.Context) error {
	select {
	default:
	case <-ctx.Done():
		return ctx.Err()
	}
	if mc.watcher == nil {
		return nil
	}

	mc.watcher <- ctx

	return nil
}

func (mc *mysqlConn) startWatcher() {
	watcher := make(chan mysqlContext, 1)
	mc.watcher = watcher
	finished := make(chan struct{})
	mc.finished = finished
	go func() {
		for {
			var ctx mysqlContext
			select {
			case ctx = <-watcher:
			case <-mc.closech:
				return
			}

			select {
			case <-ctx.Done():
				mc.cancel(ctx.Err())
			case <-finished:
			case <-mc.closech:
				return
			}
		}
	}()
}

func namedValueToValue(named []driver.NamedValue) ([]driver.Value, error) {
	dargs := make([]driver.Value, len(named))
	for n, param := range named {
		if len(param.Name) > 0 {
			return nil, errors.New("mysql: driver does not support the use of Named Parameters")
		}
		dargs[n] = param.Value
	}
	return dargs, nil
}

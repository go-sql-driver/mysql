// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2012 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"database/sql/driver"
	"io"
	"reflect"
	"strconv"
)

type mysqlStmt struct {
	mc         *mysqlConn
	id         uint32
	paramCount int
}

func (stmt *mysqlStmt) Close() error {
	if stmt.mc == nil || stmt.mc.closed.IsSet() {
		// driver.Stmt.Close can be called more than once, thus this function
		// has to be idempotent.
		// See also Issue #450 and golang/go#16019.
		//errLog.Print(ErrInvalidConn)
		return driver.ErrBadConn
	}

	err := stmt.mc.writeCommandPacketUint32(comStmtClose, stmt.id)
	stmt.mc = nil
	return err
}

func (stmt *mysqlStmt) NumInput() int {
	return stmt.paramCount
}

func (stmt *mysqlStmt) ColumnConverter(idx int) driver.ValueConverter {
	return converter{}
}

func (stmt *mysqlStmt) Exec(args []driver.Value) (driver.Result, error) {
	if stmt.mc.closed.IsSet() {
		errLog.Print(ErrInvalidConn)
		return nil, driver.ErrBadConn
	}
	// Send command
	err := stmt.writeExecutePacket(args)
	if err != nil {
		return nil, stmt.mc.markBadConn(err)
	}

	mc := stmt.mc

	mc.affectedRows = 0
	mc.insertId = 0

	// Read Result
	resLen, err := mc.readResultSetHeaderPacket()
	if err != nil {
		return nil, err
	}

	if resLen > 0 {
		// Columns
		if err = mc.readUntilEOF(); err != nil {
			return nil, err
		}

		// Rows
		if err := mc.readUntilEOF(); err != nil {
			return nil, err
		}
	}

	if err := mc.discardResults(); err != nil {
		return nil, err
	}

	return &mysqlResult{
		affectedRows: int64(mc.affectedRows),
		insertId:     int64(mc.insertId),
	}, nil
}

func (stmt *mysqlStmt) Query(args []driver.Value) (driver.Rows, error) {
	return stmt.query(args)
}

func (stmt *mysqlStmt) query(args []driver.Value) (*binaryRows, error) {
	if stmt.mc.closed.IsSet() {
		errLog.Print(ErrInvalidConn)
		return nil, driver.ErrBadConn
	}
	// Send command
	err := stmt.writeExecutePacket(args)
	if err != nil {
		return nil, stmt.mc.markBadConn(err)
	}

	mc := stmt.mc

	// Read Result
	resLen, err := mc.readResultSetHeaderPacket()
	if err != nil {
		return nil, err
	}

	rows := new(binaryRows)

	if resLen > 0 {
		rows.mc = mc
		rows.rs.columns, err = mc.readColumns(resLen)
	} else {
		rows.rs.done = true

		switch err := rows.NextResultSet(); err {
		case nil, io.EOF:
			return rows, nil
		default:
			return nil, err
		}
	}

	return rows, err
}

type converter struct{}

// ConvertValue differs from defaultConverter.ConverValue for uint64 with the high bit set only
// all other conversion requests return driver.ErrSkip to defer to the default converter
func (c converter) ConvertValue(v interface{}) (driver.Value, error) {
	if driver.IsValue(v) {
		return v, nil
	}

	// even when uint64 is the underlying type, a custom Valuer should take precedence
	if _, ok := v.(driver.Valuer); ok {
		return v, driver.ErrSkip
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Ptr:
		if rv.IsNil() {
			return nil, nil
		}
		// recursively handle *uint64, **uint64 etc
		return c.ConvertValue(rv.Elem().Interface())
	case reflect.Uint64:
		u64 := rv.Uint()
		if u64 >= 1<<63 {
			// The defaultConverter errors in this case - we convert to a string
			return strconv.FormatUint(u64, 10), nil
		}
	}

	return v, driver.ErrSkip
}

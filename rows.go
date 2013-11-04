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
)

type mysqlField struct {
	fieldType byte
	flags     fieldFlag
	name      string
}

type mysqlRows struct {
	mc      *mysqlConn
	columns []mysqlField
}

type binaryRows struct {
	mysqlRows
}

type textRows struct {
	mysqlRows
}

func (rows *mysqlRows) Columns() []string {
	columns := make([]string, len(rows.columns))
	for i := range columns {
		columns[i] = rows.columns[i].name
	}
	return columns
}

func (rows *mysqlRows) Close() error {
	if mc := rows.mc; mc == nil || mc.netConn == nil {
		rows.mc = nil
		return driver.ErrBadConn
	}

	// Remove unread packets from stream
	err := rows.mc.readUntilEOF()
	rows.mc = nil
	return err
}

func (rows *binaryRows) Next(dest []driver.Value) error {
	if mc := rows.mc; mc == nil || mc.netConn == nil {
		errLog.Print(errInvalidConn)
		rows.mc = nil
		return driver.ErrBadConn
	}

	// Fetch next row from stream
	if err := rows.readRow(dest); err != io.EOF {
		return err
	}
	rows.mc = nil
	return io.EOF
}

func (rows *textRows) Next(dest []driver.Value) error {
	if mc := rows.mc; mc == nil || mc.netConn == nil {
		errLog.Print(errInvalidConn)
		rows.mc = nil
		return driver.ErrBadConn
	}

	// Fetch next row from stream
	if err := rows.readRow(dest); err != io.EOF {
		return err
	}
	rows.mc = nil
	return io.EOF
}

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
	name      string
	fieldType byte
	flags     fieldFlag
}

type mysqlRows struct {
	mc      *mysqlConn
	binary  bool
	columns []mysqlField
	eof     bool
}

func (rows *mysqlRows) Columns() (columns []string) {
	columns = make([]string, len(rows.columns))
	for i := range columns {
		columns[i] = rows.columns[i].name
	}
	return
}

func (rows *mysqlRows) Close() (err error) {
	// Remove unread packets from stream
	if !rows.eof {
		if rows.mc == nil || rows.mc.netConn == nil {
			return errInvalidConn
		}

		err = rows.mc.readUntilEOF()

		// explicitly set because readUntilEOF might return early in case of an
		// error
		rows.eof = true
	}

	rows.mc = nil

	return
}

func (rows *mysqlRows) Next(dest []driver.Value) (err error) {
	if rows.eof {
		return io.EOF
	}

	if rows.mc == nil || rows.mc.netConn == nil {
		return errInvalidConn
	}

	// Fetch next row from stream
	if rows.binary {
		err = rows.readBinaryRow(dest)
	} else {
		err = rows.readRow(dest)
	}

	if err == io.EOF {
		rows.eof = true
	}
	return err
}

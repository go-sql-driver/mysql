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
	binary  bool
}

func (rows *mysqlRows) Columns() []string {
	columns := make([]string, len(rows.columns))
	for i := range columns {
		columns[i] = rows.columns[i].name
	}
	return columns
}

func (rows *mysqlRows) Close() error {
	mc := rows.mc
	if mc == nil {
		return nil
	}
	if mc.netConn == nil {
		return errInvalidConn
	}
	// Remove unread packets from stream
	err := mc.readUntilEOF()
	rows.mc = nil
	return err
}

func (rows *mysqlRows) Next(dest []driver.Value) error {
	mc := rows.mc
	if mc == nil {
		return io.EOF
	}
	if mc.netConn == nil {
		return errInvalidConn
	}
	var err error
	// Fetch next row from stream
	if rows.binary {
		err = rows.readBinaryRow(dest)
	} else {
		err = rows.readRow(dest)
	}
	if err == io.EOF {
		rows.mc = nil
	}
	return err
}

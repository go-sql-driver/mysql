// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2012 Julien Schmidt. All rights reserved.
// http://www.julienschmidt.com
// 
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"database/sql/driver"
	"errors"
	"io"
)

type mysqlField struct {
	name      string
	fieldType FieldType
	flags     FieldFlag
}

type rowsContent struct {
	mc      *mysqlConn
	binary  bool
	columns []mysqlField
	eof     bool
}

type mysqlRows struct {
	content *rowsContent
}

func (rows mysqlRows) Columns() (columns []string) {
	columns = make([]string, len(rows.content.columns))
	for i := 0; i < cap(columns); i++ {
		columns[i] = rows.content.columns[i].name
	}
	return
}

func (rows mysqlRows) Close() (e error) {
	defer func() {
		rows.content.mc = nil
		rows.content = nil
	}()

	// Remove unread packets from stream
	if !rows.content.eof {
		if rows.content.mc == nil {
			return errors.New("Invalid Connection")
		}

		_, e = rows.content.mc.readUntilEOF()
		if e != nil {
			return
		}
	}

	return nil
}

// Next returns []driver.Value filled with either nil values for NULL entries
// or []byte's for all other entries. Type conversion is done on rows.scan(),
// when the dest type is know, which makes type conversion easier and avoids 
// unnecessary conversions.
func (rows mysqlRows) Next(dest []driver.Value) error {
	if rows.content.eof {
		return io.EOF
	}

	if rows.content.mc == nil {
		return errors.New("Invalid Connection")
	}

	columnsCount := cap(dest)

	// Fetch next row from stream
	var row *[]*[]byte
	var e error
	if rows.content.binary {
		row, e = rows.content.mc.readBinaryRow(rows.content)
	} else {
		row, e = rows.content.mc.readRow(columnsCount)
	}

	if e != nil {
		if e == io.EOF {
			rows.content.eof = true
		}
		return e
	}

	for i := 0; i < columnsCount; i++ {
		if (*row)[i] == nil {
			dest[i] = nil
		} else {
			dest[i] = *(*row)[i]
		}
	}

	return nil
}

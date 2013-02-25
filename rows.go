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

type mysqlRows struct {
	mc      *mysqlConn
	binary  bool
	columns []mysqlField
	eof     bool
}

func (rows *mysqlRows) Columns() (columns []string) {
	columns = make([]string, len(rows.columns))
	for i := 0; i < cap(columns); i++ {
		columns[i] = rows.columns[i].name
	}
	return
}

func (rows *mysqlRows) Close() (err error) {
	defer func() {
		rows.mc = nil
	}()

	// Remove unread packets from stream
	if !rows.eof {
		if rows.mc == nil {
			return errors.New("Invalid Connection")
		}

		_, err = rows.mc.readUntilEOF()
		if err != nil {
			return
		}
	}

	return nil
}

// Next returns []driver.Value filled with either nil values for NULL entries
// or []byte's for all other entries. Type conversion is done on rows.scan(),
// when the dest type is know, which makes type conversion easier and avoids
// unnecessary conversions.
func (rows *mysqlRows) Next(dest []driver.Value) error {
	if rows.eof {
		return io.EOF
	}

	if rows.mc == nil {
		return errors.New("Invalid Connection")
	}

	columnsCount := cap(dest)

	// Fetch next row from stream
	var row *[]*[]byte
	var err error
	if rows.binary {
		row, err = rows.mc.readBinaryRow(rows)
	} else {
		row, err = rows.mc.readRow(columnsCount)
	}

	if err != nil {
		if err == io.EOF {
			rows.eof = true
		}
		return err
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

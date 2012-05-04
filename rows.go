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
	"io"
)

type mysqlField struct {
	name      string
	fieldType FieldType
	flags     FieldFlag
}

type rowsContent struct {
	columns []*mysqlField
	rows    []*[]*[]byte
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

func (rows mysqlRows) Close() error {
	rows.content = nil
	return nil
}

// Next returns []driver.Value filled with either nil values for NULL entries
// or []byte's for all other entries. Type conversion is done on rows.scan(),
// when the dest. type is know, which makes type conversion easier and avoids 
// unnecessary conversions.
func (rows mysqlRows) Next(dest []driver.Value) error {
	if len(rows.content.rows) > 0 {
		var value *[]byte
		for i := 0; i < cap(dest); i++ {
			value = (*rows.content.rows[0])[i]

			if value == nil {
				dest[i] = nil
			} else {
				dest[i] = *value
			}
		}
		rows.content.rows = rows.content.rows[1:]
	} else {
		return io.EOF
	}
	return nil
}

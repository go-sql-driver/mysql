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
)

type stmtContent struct {
	mc         *mysqlConn
	id         uint32
	paramCount int
	params     []mysqlField
}

type mysqlStmt struct {
	*stmtContent
}

func (stmt mysqlStmt) Close() error {
	e := stmt.mc.writeCommandPacket(COM_STMT_CLOSE, stmt.id)
	stmt.mc = nil
	return e
}

func (stmt mysqlStmt) NumInput() int {
	return stmt.paramCount
}

func (stmt mysqlStmt) Exec(args []driver.Value) (driver.Result, error) {
	if stmt.mc == nil {
		return nil, errors.New(`Invalid Statement`)
	}
	stmt.mc.affectedRows = 0
	stmt.mc.insertId = 0

	// Send command
	e := stmt.buildExecutePacket(&args)
	if e != nil {
		return nil, e
	}

	// Read Result
	var resLen int
	resLen, e = stmt.mc.readResultSetHeaderPacket()
	if e != nil {
		return nil, e
	}

	if resLen > 0 {
		// Columns
		_, e = stmt.mc.readUntilEOF()
		if e != nil {
			return nil, e
		}

		// Rows
		stmt.mc.affectedRows, e = stmt.mc.readUntilEOF()
		if e != nil {
			return nil, e
		}
	}
	if e != nil {
		return nil, e
	}

	if stmt.mc.affectedRows == 0 {
		return driver.ResultNoRows, nil
	}

	return mysqlResult{
			affectedRows: int64(stmt.mc.affectedRows),
			insertId:     int64(stmt.mc.insertId)},
		nil
}

func (stmt mysqlStmt) Query(args []driver.Value) (dr driver.Rows, e error) {
	if stmt.mc == nil {
		return nil, errors.New(`Invalid Statement`)
	}

	// Send command
	e = stmt.buildExecutePacket(&args)
	if e != nil {
		return nil, e
	}

	// Get Result
	var resLen int
	rows := mysqlRows{new(rowsContent)}
	resLen, e = stmt.mc.readResultSetHeaderPacket()
	if e != nil {
		return nil, e
	}

	if resLen > 0 {
		// Columns
		rows.content.columns, e = stmt.mc.readColumns(resLen)
		if e != nil {
			return
		}

		// Rows
		e = stmt.mc.readBinaryRows(rows.content)
		if e != nil {
			return
		}
	}

	dr = rows
	return
}

// ColumnConverter returns a ValueConverter for the provided
// column index.  If the type of a specific column isn't known
// or shouldn't be handled specially, DefaultValueConverter
// can be returned.
//func (stmt mysqlStmt) ColumnConverter(idx int) driver.ValueConverter {
//	debug(fmt.Sprintf("ColumnConverter(%d)", idx))
//	return driver.DefaultParameterConverter
//}

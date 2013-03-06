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
)

type mysqlStmt struct {
	mc         *mysqlConn
	id         uint32
	paramCount int
	params     []mysqlField
}

func (stmt *mysqlStmt) Close() (err error) {
	err = stmt.mc.writeCommandPacketUint32(comStmtClose, stmt.id)
	stmt.mc = nil
	return
}

func (stmt *mysqlStmt) NumInput() int {
	return stmt.paramCount
}

func (stmt *mysqlStmt) Exec(args []driver.Value) (driver.Result, error) {
	stmt.mc.affectedRows = 0
	stmt.mc.insertId = 0

	// Send command
	err := stmt.writeExecutePacket(args)
	if err != nil {
		return nil, err
	}

	// Read Result
	var resLen int
	resLen, err = stmt.mc.readResultSetHeaderPacket()
	if err == nil {
		if resLen > 0 {
			// Columns
			err = stmt.mc.readUntilEOF()
			if err != nil {
				return nil, err
			}

			// Rows
			err = stmt.mc.readUntilEOF()
		}
		if err == nil {
			return &mysqlResult{
				affectedRows: int64(stmt.mc.affectedRows),
				insertId:     int64(stmt.mc.insertId),
			}, nil
		}
	}

	return nil, err
}

func (stmt *mysqlStmt) Query(args []driver.Value) (driver.Rows, error) {
	// Send command
	err := stmt.writeExecutePacket(args)
	if err != nil {
		return nil, err
	}

	// Read Result
	var resLen int
	resLen, err = stmt.mc.readResultSetHeaderPacket()
	if err != nil {
		return nil, err
	}

	rows := &mysqlRows{stmt.mc, true, nil, false}

	if resLen > 0 {
		// Columns
		rows.columns, err = stmt.mc.readColumns(resLen)
		if err != nil {
			return nil, err
		}
	}

	return rows, err
}

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
	"net"
	"strings"
)

type mysqlConn struct {
	cfg          *config
	flags        ClientFlag
	charset      byte
	cipher       []byte
	netConn      net.Conn
	buf          *buffer
	protocol     uint8
	sequence     uint8
	affectedRows uint64
	insertId     uint64
}

type config struct {
	user   string
	passwd string
	net    string
	addr   string
	dbname string
	params map[string]string
}

// Handles parameters set in DSN
func (mc *mysqlConn) handleParams() (err error) {
	for param, val := range mc.cfg.params {
		switch param {
		// Charset
		case "charset":
			charsets := strings.Split(val, ",")
			for _, charset := range charsets {
				err = mc.exec("SET NAMES " + charset)
				if err != nil {
					return
				}
			}

		// TLS-Encryption
		case "tls":
			err = errors.New("TLS-Encryption not implemented yet")
			return

		// Compression
		case "compress":
			err = errors.New("Compression not implemented yet")

		// System Vars
		default:
			err = mc.exec("SET " + param + "=" + val + "")
			if err != nil {
				return
			}
		}
	}

	return
}

func (mc *mysqlConn) Begin() (driver.Tx, error) {
	err := mc.exec("START TRANSACTION")
	if err == nil {
		return &mysqlTx{mc}, err
	}

	return nil, err
}

func (mc *mysqlConn) Close() (err error) {
	mc.writeCommandPacket(COM_QUIT)
	mc.cfg = nil
	mc.buf = nil
	mc.netConn.Close()
	mc.netConn = nil
	return
}

func (mc *mysqlConn) Prepare(query string) (driver.Stmt, error) {
	// Send command
	err := mc.writeCommandPacketStr(COM_STMT_PREPARE, query)
	if err != nil {
		return nil, err
	}

	stmt := &mysqlStmt{
		mc: mc,
	}

	// Read Result
	columnCount, err := stmt.readPrepareResultPacket()
	if err == nil {
		if stmt.paramCount > 0 {
			stmt.params, err = stmt.mc.readColumns(stmt.paramCount)
			if err != nil {
				return nil, err
			}
		}

		if columnCount > 0 {
			err = stmt.mc.readUntilEOF()
		}
	}

	return stmt, err
}

func (mc *mysqlConn) Exec(query string, args []driver.Value) (_ driver.Result, err error) {
	if len(args) > 0 { // with args, must use prepared stmt
		var res driver.Result
		var stmt driver.Stmt
		stmt, err = mc.Prepare(query)
		if err == nil {
			res, err = stmt.Exec(args)
			if err == nil {
				return res, stmt.Close()
			}
		}
	} else { // no args, fastpath
		mc.affectedRows = 0
		mc.insertId = 0

		err = mc.exec(query)
		if err == nil {
			return &mysqlResult{
				affectedRows: int64(mc.affectedRows),
				insertId:     int64(mc.insertId),
			}, err
		}
	}
	return nil, err

}

// Internal function to execute commands
func (mc *mysqlConn) exec(query string) (err error) {
	// Send command
	err = mc.writeCommandPacketStr(COM_QUERY, query)
	if err != nil {
		return
	}

	// Read Result
	var resLen int
	resLen, err = mc.readResultSetHeaderPacket()
	if err == nil && resLen > 0 {
		err = mc.readUntilEOF()
		if err != nil {
			return
		}

		err = mc.readUntilEOF()
	}

	return
}

func (mc *mysqlConn) Query(query string, args []driver.Value) (_ driver.Rows, err error) {
	if len(args) > 0 { // with args, must use prepared stmt
		var rows driver.Rows
		var stmt driver.Stmt
		stmt, err = mc.Prepare(query)
		if err == nil {
			rows, err = stmt.Query(args)
			if err == nil {
				return rows, stmt.Close()
			}
		}
		return
	} else { // no args, fastpath
		var rows *mysqlRows
		// Send command
		err = mc.writeCommandPacketStr(COM_QUERY, query)
		if err == nil {
			// Read Result
			var resLen int
			resLen, err = mc.readResultSetHeaderPacket()
			if err == nil {
				rows = &mysqlRows{mc, false, nil, false}

				if resLen > 0 {
					// Columns
					rows.columns, err = mc.readColumns(resLen)
				}
				return rows, err
			}
		}
	}

	return nil, err
}

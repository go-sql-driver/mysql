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
	"bufio"
	"database/sql/driver"
	"net"
	"strings"
)

type mysqlConn struct {
	cfg          *config
	server       *serverSettings
	netConn      net.Conn
	bufReader    *bufio.Reader
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

type serverSettings struct {
	protocol     byte
	version      string
	flags        ClientFlag
	charset      uint8
	scrambleBuff []byte
	threadID     uint32
}

// Handles parameters set in DSN
func (mc *mysqlConn) handleParams() (e error) {
	for param, val := range mc.cfg.params {
		switch param {
		// Charset
		case "charset":
			charsets := strings.Split(val, ",")
			for _, charset := range charsets {
				e = mc.exec("SET NAMES " + charset)
				if e == nil {
					break
				}
			}
			if e != nil {
				return
			}

		// TLS-Encryption
		case "tls":
			dbgLog.Print("TLS-Encryption not implemented yet")

		// Compression
		case "compress":
			dbgLog.Print("Compression not implemented yet")

		// System Vars
		default:
			e = mc.exec("SET " + param + "=" + val + "")
			if e != nil {
				return
			}
		}
	}

	return
}

func (mc *mysqlConn) Begin() (driver.Tx, error) {
	e := mc.exec("START TRANSACTION")
	if e != nil {
		return nil, e
	}

	return &mysqlTx{mc}, e
}

func (mc *mysqlConn) Close() (e error) {
	mc.writeCommandPacket(COM_QUIT)
	mc.bufReader = nil
	mc.netConn.Close()
	mc.netConn = nil
	return
}

func (mc *mysqlConn) Prepare(query string) (driver.Stmt, error) {
	// Send command
	e := mc.writeCommandPacket(COM_STMT_PREPARE, query)
	if e != nil {
		return nil, e
	}

	stmt := mysqlStmt{new(stmtContent)}
	stmt.mc = mc

	// Read Result
	var columnCount uint16
	columnCount, e = stmt.readPrepareResultPacket()
	if e != nil {
		return nil, e
	}

	if stmt.paramCount > 0 {
		stmt.params, e = stmt.mc.readColumns(stmt.paramCount)
		if e != nil {
			return nil, e
		}
	}

	if columnCount > 0 {
		_, e = stmt.mc.readUntilEOF()
		if e != nil {
			return nil, e
		}
	}

	return stmt, e
}

func (mc *mysqlConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	if len(args) > 0 {
		return nil, driver.ErrSkip
	}

	mc.affectedRows = 0
	mc.insertId = 0

	e := mc.exec(query)
	if e != nil {
		return nil, e
	}

	return &mysqlResult{
			affectedRows: int64(mc.affectedRows),
			insertId:     int64(mc.insertId)},
		e
}

// Internal function to execute commands
func (mc *mysqlConn) exec(query string) (e error) {
	// Send command
	e = mc.writeCommandPacket(COM_QUERY, query)
	if e != nil {
		return
	}

	// Read Result
	var resLen int
	resLen, e = mc.readResultSetHeaderPacket()
	if e != nil {
		return
	}

	if resLen > 0 {
		_, e = mc.readUntilEOF()
		if e != nil {
			return
		}

		mc.affectedRows, e = mc.readUntilEOF()
		if e != nil {
			return
		}
	}

	return
}

func (mc *mysqlConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	if len(args) > 0 {
		return nil, driver.ErrSkip
	}

	// Send command
	e := mc.writeCommandPacket(COM_QUERY, query)
	if e != nil {
		return nil, e
	}

	// Read Result
	var resLen int
	resLen, e = mc.readResultSetHeaderPacket()
	if e != nil {
		return nil, e
	}

	rows := mysqlRows{&rowsContent{mc, false, nil, false}}

	if resLen > 0 {
		// Columns
		rows.content.columns, e = mc.readColumns(resLen)
		if e != nil {
			return nil, e
		}
	}

	return rows, e
}

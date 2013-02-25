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
	"errors"
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
func (mc *mysqlConn) handleParams() (err error) {
	for param, val := range mc.cfg.params {
		switch param {
		// Charset
		case "charset":
			charsets := strings.Split(val, ",")
			for _, charset := range charsets {
				err = mc.exec("SET NAMES " + charset)
				if err == nil {
					break
				}
			}
			if err != nil {
				return
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
	if err != nil {
		return nil, err
	}

	return &mysqlTx{mc}, err
}

func (mc *mysqlConn) Close() (err error) {
	mc.writeCommandPacket(COM_QUIT)
	mc.bufReader = nil
	mc.netConn.Close()
	mc.netConn = nil
	return
}

func (mc *mysqlConn) Prepare(query string) (driver.Stmt, error) {
	// Send command
	err := mc.writeCommandPacket(COM_STMT_PREPARE, query)
	if err != nil {
		return nil, err
	}

	stmt := &mysqlStmt{
		mc: mc,
	}

	// Read Result
	var columnCount uint16
	columnCount, err = stmt.readPrepareResultPacket()
	if err != nil {
		return nil, err
	}

	if stmt.paramCount > 0 {
		stmt.params, err = stmt.mc.readColumns(stmt.paramCount)
		if err != nil {
			return nil, err
		}
	}

	if columnCount > 0 {
		_, err = stmt.mc.readUntilEOF()
		if err != nil {
			return nil, err
		}
	}

	return stmt, err
}

func (mc *mysqlConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	if len(args) > 0 {
		return nil, driver.ErrSkip
	}

	mc.affectedRows = 0
	mc.insertId = 0

	err := mc.exec(query)
	if err != nil {
		return nil, err
	}

	return &mysqlResult{
			affectedRows: int64(mc.affectedRows),
			insertId:     int64(mc.insertId)},
		err
}

// Internal function to execute commands
func (mc *mysqlConn) exec(query string) (err error) {
	// Send command
	err = mc.writeCommandPacket(COM_QUERY, query)
	if err != nil {
		return
	}

	// Read Result
	var resLen int
	resLen, err = mc.readResultSetHeaderPacket()
	if err != nil {
		return
	}

	if resLen > 0 {
		_, err = mc.readUntilEOF()
		if err != nil {
			return
		}

		if mc.affectedRows > 0 {
			_, err = mc.readUntilEOF()
			return
		}

		mc.affectedRows, err = mc.readUntilEOF()
		return
	}

	return
}

func (mc *mysqlConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	if len(args) > 0 {
		return nil, driver.ErrSkip
	}

	// Send command
	err := mc.writeCommandPacket(COM_QUERY, query)
	if err != nil {
		return nil, err
	}

	// Read Result
	var resLen int
	resLen, err = mc.readResultSetHeaderPacket()
	if err != nil {
		return nil, err
	}

	rows := &mysqlRows{mc, false, nil, false}

	if resLen > 0 {
		// Columns
		rows.columns, err = mc.readColumns(resLen)
		if err != nil {
			return nil, err
		}
	}

	return rows, err
}

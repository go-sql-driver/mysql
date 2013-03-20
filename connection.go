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
	cfg              *config
	flags            clientFlag
	charset          byte
	cipher           []byte
	netConn          net.Conn
	buf              *buffer
	protocol         uint8
	sequence         uint8
	affectedRows     uint64
	insertId         uint64
	maxPacketAllowed int
	maxWriteSize     int
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
			for i := range charsets {
				// ignore errors here - a charset may not exist
				err = mc.exec("SET NAMES " + charsets[i])
				if err == nil {
					break
				}
			}
			if err != nil {
				return
			}

		// handled elsewhere
		case "timeout", "allowAllFiles":
			continue

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
	mc.writeCommandPacket(comQuit)
	mc.cfg = nil
	mc.buf = nil
	mc.netConn.Close()
	mc.netConn = nil
	return
}

func (mc *mysqlConn) Prepare(query string) (driver.Stmt, error) {
	// Send command
	err := mc.writeCommandPacketStr(comStmtPrepare, query)
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

func (mc *mysqlConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	if len(args) == 0 { // no args, fastpath
		mc.affectedRows = 0
		mc.insertId = 0

		err := mc.exec(query)
		if err == nil {
			return &mysqlResult{
				affectedRows: int64(mc.affectedRows),
				insertId:     int64(mc.insertId),
			}, err
		}
		return nil, err
	}

	// with args, must use prepared stmt
	return nil, driver.ErrSkip

}

// Internal function to execute commands
func (mc *mysqlConn) exec(query string) (err error) {
	// Send command
	err = mc.writeCommandPacketStr(comQuery, query)
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

func (mc *mysqlConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	if len(args) == 0 { // no args, fastpath
		// Send command
		err := mc.writeCommandPacketStr(comQuery, query)
		if err == nil {
			// Read Result
			var resLen int
			resLen, err = mc.readResultSetHeaderPacket()
			if err == nil {
				rows := &mysqlRows{mc, false, nil, false}

				if resLen > 0 {
					// Columns
					rows.columns, err = mc.readColumns(resLen)
				}
				return rows, err
			}
		}

		return nil, err
	}

	// with args, must use prepared stmt
	return nil, driver.ErrSkip
}

// Gets the value of the given MySQL System Variable
func (mc *mysqlConn) getSystemVar(name string) (val []byte, err error) {
	// Send command
	err = mc.writeCommandPacketStr(comQuery, "SELECT @@"+name)
	if err == nil {
		// Read Result
		var resLen int
		resLen, err = mc.readResultSetHeaderPacket()
		if err == nil {
			rows := &mysqlRows{mc, false, nil, false}

			if resLen > 0 {
				// Columns
				rows.columns, err = mc.readColumns(resLen)
			}

			dest := make([]driver.Value, resLen)
			err = rows.readRow(dest)
			if err == nil {
				val = dest[0].([]byte)
				err = mc.readUntilEOF()
			}
		}
	}

	return
}

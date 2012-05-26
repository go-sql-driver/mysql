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
	"strconv"
	"time"
)

type mysqlConn struct {
	cfg            *config
	server         *serverSettings
	netConn        net.Conn
	bufReader      *bufio.Reader
	protocol       uint8
	sequence       uint8
	affectedRows   uint64
	insertId       uint64
	lastCmdTime    time.Time
	keepaliveTimer *time.Timer
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
	keepalive    int64
}

// Handles parameters set in DSN
func (mc *mysqlConn) handleParams() (e error) {
	for param, val := range mc.cfg.params {
		switch param {
		// Charset
		case "charset":
			e = mc.exec("SET NAMES " + val)
			if e != nil {
				return
			}

		// TLS-Encryption
		case "tls":
			dbgLog.Print("TLS-Encryption not implemented yet")

		// Compression
		case "compress":
			dbgLog.Print("Compression not implemented yet")

		// We don't want to set keepalive as system var
		case "keepalive":
			continue

		// System Vars
		default:
			e = mc.exec("SET " + param + "=" + val + "")
			if e != nil {
				return
			}
		}
	}

	// KeepAlive
	if val, param := mc.cfg.params["keepalive"]; param {
		mc.server.keepalive, e = strconv.ParseInt(val, 10, 64)
		if e != nil {
			return errors.New("Invalid keepalive time")
		}

		// Get keepalive time by MySQL system var wait_timeout
		if mc.server.keepalive == 1 {
			val, e = mc.getSystemVar("wait_timeout")
			mc.server.keepalive, e = strconv.ParseInt(val, 10, 64)
			if e != nil {
				return errors.New("Error getting wait_timeout")
			}

			// Trigger 1min BEFORE wait_timeout
			if mc.server.keepalive > 60 {
				mc.server.keepalive -= 60
			}
		}

		if mc.server.keepalive > 0 {
			mc.lastCmdTime = time.Now()

			// Ping-Timer to avoid timeout
			mc.keepaliveTimer = time.AfterFunc(
				time.Duration(mc.server.keepalive)*time.Second, func() {
					var diff time.Duration
					for {
						// Fires only if diff > keepalive. Makes it collision safe
						for mc.netConn != nil &&
							mc.lastCmdTime.Unix()+mc.server.keepalive > time.Now().Unix() {
							diff = mc.lastCmdTime.Sub(time.Unix(time.Now().Unix()-mc.server.keepalive, 0))
							time.Sleep(diff)
						}
						if mc.netConn != nil {
							if e := mc.Ping(); e != nil {
								break
							}
						} else {
							return
						}
					}
				})
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
	if mc.server.keepalive > 0 {
		mc.keepaliveTimer.Stop()
	}
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

	if mc.affectedRows == 0 {
		return driver.ResultNoRows, e
	}

	return &mysqlResult{
			affectedRows: int64(mc.affectedRows),
			insertId:     int64(mc.insertId)},
		e
}

// Internal function to execute statements
func (mc *mysqlConn) exec(query string) (e error) {
	// Send command
	e = mc.writeCommandPacket(COM_QUERY, query)
	if e != nil {
		return
	}

	// Read Result
	resLen, e := mc.readResultSetHeaderPacket()
	if e != nil {
		return
	}

	mc.affectedRows = 0
	mc.insertId = 0

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

// Gets the value of the given MySQL System Variable
func (mc *mysqlConn) getSystemVar(name string) (val string, e error) {
	// Send command
	e = mc.writeCommandPacket(COM_QUERY, "SELECT @@"+name)
	if e != nil {
		return
	}

	// Read Result
	resLen, e := mc.readResultSetHeaderPacket()
	if e != nil {
		return
	}

	if resLen > 0 {
		var n uint64
		n, e = mc.readUntilEOF()
		if e != nil {
			return
		}

		var rows []*[][]byte
		rows, e = mc.readRows(int(n))
		if e != nil {
			return
		}

		val = string((*rows[0])[0])
	}

	return
}

// Executes a simple Ping-CMD to test or keepalive the connection
func (mc *mysqlConn) Ping() (e error) {
	// Send command
	e = mc.writeCommandPacket(COM_PING)
	if e != nil {
		return
	}

	// Read Result
	e = mc.readResultOK()
	return
}

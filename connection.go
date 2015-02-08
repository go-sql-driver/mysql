// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2012 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"crypto/tls"
	"database/sql/driver"
	"errors"
	"net"
	"strconv"
	"strings"
	"time"
)

type mysqlConn struct {
	buf              buffer
	netConn          net.Conn
	affectedRows     uint64
	insertId         uint64
	cfg              *config
	maxPacketAllowed int
	maxWriteSize     int
	flags            clientFlag
	status           statusFlag
	sequence         uint8
	parseTime        bool
	strict           bool
}

type config struct {
	user              string
	passwd            string
	net               string
	addr              string
	dbname            string
	params            map[string]string
	loc               *time.Location
	tls               *tls.Config
	timeout           time.Duration
	collation         uint8
	allowAllFiles     bool
	allowOldPasswords bool
	clientFoundRows   bool
	columnsWithAlias  bool
	interpolateParams bool
}

// Handles parameters set in DSN after the connection is established
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

		// time.Time parsing
		case "parseTime":
			var isBool bool
			mc.parseTime, isBool = readBool(val)
			if !isBool {
				return errors.New("Invalid Bool value: " + val)
			}

		// Strict mode
		case "strict":
			var isBool bool
			mc.strict, isBool = readBool(val)
			if !isBool {
				return errors.New("Invalid Bool value: " + val)
			}

		// Compression
		case "compress":
			err = errors.New("Compression not implemented yet")
			return

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
	if mc.netConn == nil {
		errLog.Print(ErrInvalidConn)
		return nil, driver.ErrBadConn
	}
	err := mc.exec("START TRANSACTION")
	if err == nil {
		return &mysqlTx{mc}, err
	}

	return nil, err
}

func (mc *mysqlConn) Close() (err error) {
	// Makes Close idempotent
	if mc.netConn != nil {
		err = mc.writeCommandPacket(comQuit)
		if err == nil {
			err = mc.netConn.Close()
		} else {
			mc.netConn.Close()
		}
		mc.netConn = nil
	}

	mc.cfg = nil
	mc.buf.rd = nil

	return
}

func (mc *mysqlConn) Prepare(query string) (driver.Stmt, error) {
	if mc.netConn == nil {
		errLog.Print(ErrInvalidConn)
		return nil, driver.ErrBadConn
	}
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
			if err = mc.readUntilEOF(); err != nil {
				return nil, err
			}
		}

		if columnCount > 0 {
			err = mc.readUntilEOF()
		}
	}

	return stmt, err
}

// https://github.com/mysql/mysql-server/blob/mysql-5.7.5/libmysql/libmysql.c#L1150-L1156
func (mc *mysqlConn) escapeBytes(v []byte) string {
	var escape func([]byte) []byte
	if mc.status&statusNoBackslashEscapes == 0 {
		escape = EscapeString
	} else {
		escape = EscapeQuotes
	}
	return "'" + string(escape(v)) + "'"
}

func (mc *mysqlConn) interpolateParams(query string, args []driver.Value) (string, error) {
	chunks := strings.Split(query, "?")
	if len(chunks) != len(args)+1 {
		return "", driver.ErrSkip
	}

	parts := make([]string, len(chunks)+len(args))
	parts[0] = chunks[0]

	for i, arg := range args {
		pos := i*2 + 1
		parts[pos+1] = chunks[i+1]
		if arg == nil {
			parts[pos] = "NULL"
			continue
		}
		switch v := arg.(type) {
		case int64:
			parts[pos] = strconv.FormatInt(v, 10)
		case float64:
			parts[pos] = strconv.FormatFloat(v, 'g', -1, 64)
		case bool:
			if v {
				parts[pos] = "1"
			} else {
				parts[pos] = "0"
			}
		case time.Time:
			if v.IsZero() {
				parts[pos] = "'0000-00-00'"
			} else {
				fmt := "'2006-01-02 15:04:05.999999'"
				parts[pos] = v.In(mc.cfg.loc).Format(fmt)
			}
		case []byte:
			if v == nil {
				parts[pos] = "NULL"
			} else {
				parts[pos] = mc.escapeBytes(v)
			}
		case string:
			parts[pos] = mc.escapeBytes([]byte(v))
		default:
			return "", driver.ErrSkip
		}
	}
	pktSize := len(query) + 4 // 4 bytes for header.
	for _, p := range parts {
		pktSize += len(p)
	}
	if pktSize > mc.maxPacketAllowed {
		return "", driver.ErrSkip
	}
	return strings.Join(parts, ""), nil
}

func (mc *mysqlConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	if mc.netConn == nil {
		errLog.Print(ErrInvalidConn)
		return nil, driver.ErrBadConn
	}
	if len(args) != 0 {
		if !mc.cfg.interpolateParams {
			return nil, driver.ErrSkip
		}
		// try client-side prepare to reduce roundtrip
		prepared, err := mc.interpolateParams(query, args)
		if err != nil {
			return nil, err
		}
		query = prepared
		args = nil
	}
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

// Internal function to execute commands
func (mc *mysqlConn) exec(query string) error {
	// Send command
	err := mc.writeCommandPacketStr(comQuery, query)
	if err != nil {
		return err
	}

	// Read Result
	resLen, err := mc.readResultSetHeaderPacket()
	if err == nil && resLen > 0 {
		if err = mc.readUntilEOF(); err != nil {
			return err
		}

		err = mc.readUntilEOF()
	}

	return err
}

func (mc *mysqlConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	if mc.netConn == nil {
		errLog.Print(ErrInvalidConn)
		return nil, driver.ErrBadConn
	}
	if len(args) != 0 {
		if !mc.cfg.interpolateParams {
			return nil, driver.ErrSkip
		}
		// try client-side prepare to reduce roundtrip
		prepared, err := mc.interpolateParams(query, args)
		if err != nil {
			return nil, err
		}
		query = prepared
		args = nil
	}
	// Send command
	err := mc.writeCommandPacketStr(comQuery, query)
	if err == nil {
		// Read Result
		var resLen int
		resLen, err = mc.readResultSetHeaderPacket()
		if err == nil {
			rows := new(textRows)
			rows.mc = mc

			if resLen == 0 {
				// no columns, no more data
				return emptyRows{}, nil
			}
			// Columns
			rows.columns, err = mc.readColumns(resLen)
			return rows, err
		}
	}
	return nil, err
}

// Gets the value of the given MySQL System Variable
// The returned byte slice is only valid until the next read
func (mc *mysqlConn) getSystemVar(name string) ([]byte, error) {
	// Send command
	if err := mc.writeCommandPacketStr(comQuery, "SELECT @@"+name); err != nil {
		return nil, err
	}

	// Read Result
	resLen, err := mc.readResultSetHeaderPacket()
	if err == nil {
		rows := new(textRows)
		rows.mc = mc

		if resLen > 0 {
			// Columns
			if err := mc.readUntilEOF(); err != nil {
				return nil, err
			}
		}

		dest := make([]driver.Value, resLen)
		if err = rows.readRow(dest); err == nil {
			return dest[0].([]byte), mc.readUntilEOF()
		}
	}
	return nil, err
}

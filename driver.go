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
	"database/sql"
	"database/sql/driver"
	"errors"
	"net"
)

type mysqlDriver struct{}

// Open new Connection.
// See http://code.google.com/p/go-mysql-driver/#DSN_(Data_Source_Name) for how
// the DSN string is formated
func (d *mysqlDriver) Open(dsn string) (driver.Conn, error) {
	var e error

	// New mysqlConn
	mc := new(mysqlConn)
	mc.cfg = parseDSN(dsn)

	if mc.cfg.dbname == "" {
		e = errors.New("Incomplete or invalid DSN")
		return nil, e
	}

	// Connect to Server
	mc.netConn, e = net.Dial(mc.cfg.net, mc.cfg.addr)
	if e != nil {
		return nil, e
	}

	// Reading Handshake Initialization Packet 
	e = mc.readInitPacket()
	if e != nil {
		return nil, e
	}

	// Send Client Authentication Packet
	e = mc.writeAuthPacket()
	if e != nil {
		return nil, e
	}

	// Read Result Packet
	e = mc.readResultOK()
	if e != nil {
		return nil, e
	}

	// Handle DSN Params
	e = mc.handleParams()
	if e != nil {
		return nil, e
	}

	return mc, e
}

func init() {
	sql.Register("mysql", &mysqlDriver{})
}

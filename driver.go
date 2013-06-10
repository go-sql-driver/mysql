// Copyright 2012 Julien Schmidt. All rights reserved.
// http://www.julienschmidt.com
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
package mysql

import (
	"database/sql"
	"database/sql/driver"
	"net"
)

type mysqlDriver struct{}

// Open new Connection.
// See https://github.com/go-sql-driver/mysql#dsn-data-source-name for how
// the DSN string is formated
func (d *mysqlDriver) Open(dsn string) (driver.Conn, error) {
	var err error

	// New mysqlConn
	mc := &mysqlConn{
		maxPacketAllowed: maxPacketSize,
		maxWriteSize:     maxPacketSize - 1,
	}
	mc.cfg, err = parseDSN(dsn)
	if err != nil {
		return nil, err
	}

	// Connect to Server
	nd := net.Dialer{Timeout: mc.cfg.timeout}
	mc.netConn, err = nd.Dial(mc.cfg.net, mc.cfg.addr)
	if err != nil {
		return nil, err
	}
	mc.buf = newBuffer(mc.netConn)

	// Reading Handshake Initialization Packet
	err = mc.readInitPacket()
	if err != nil {
		return nil, err
	}

	// Send Client Authentication Packet
	err = mc.writeAuthPacket()
	if err != nil {
		return nil, err
	}

	// Read Result Packet
	err = mc.readResultOK()
	if err != nil {
		return nil, err
	}

	// Get max allowed packet size
	maxap, err := mc.getSystemVar("max_allowed_packet")
	if err != nil {
		return nil, err
	}
	mc.maxPacketAllowed = stringToInt(maxap) - 1
	if mc.maxPacketAllowed < maxPacketSize {
		mc.maxWriteSize = mc.maxPacketAllowed
	}

	// Handle DSN Params
	err = mc.handleParams()
	if err != nil {
		return nil, err
	}

	return mc, err
}

func init() {
	sql.Register("mysql", &mysqlDriver{})
}

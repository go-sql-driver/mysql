// Copyright 2012 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

// Package mysql provides a MySQL driver for Go's database/sql package.
//
// The driver should be used via the database/sql package:
//
//  import "database/sql"
//  import _ "github.com/go-sql-driver/mysql"
//
//  db, err := sql.Open("mysql", "user:password@/dbname")
//
// See https://github.com/go-sql-driver/mysql#usage for details
package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"net"
)

var (
	errInvalidUser   = errors.New("invalid Connection: User is not set or longer than 32 chars")
	errInvalidAddr   = errors.New("invalid Connection: Addr config is missing")
	errInvalidNet    = errors.New("invalid Connection: Only tcp is valid for Net")
	errInvalidDBName = errors.New("invalid Connection: DBName config is missing")
)

// watcher interface is used for context support (From Go 1.8)
type watcher interface {
	startWatcher()
}

// MySQLDriver is exported to make the driver directly accessible.
// In general the driver is used via the database/sql package.
type MySQLDriver struct {
}

type MySQLConnector struct {
	Cfg *Config
}

// DialFunc is a function which can be used to establish the network connection.
// Custom dial functions must be registered with RegisterDial
type DialFunc func(addr string) (net.Conn, error)

var dials map[string]DialFunc

// RegisterDial registers a custom dial function. It can then be used by the
// network address mynet(addr), where mynet is the registered new network.
// addr is passed as a parameter to the dial function.
func RegisterDial(net string, dial DialFunc) {
	if dials == nil {
		dials = make(map[string]DialFunc)
	}
	dials[net] = dial
}

//Open a new Connection
func connectServer(cxt context.Context, mc *mysqlConn) error {
	var err error
	// Connect to Server
	if dial, ok := dials[mc.cfg.Net]; ok {
		mc.netConn, err = dial(mc.cfg.Addr)
	} else {
		nd := net.Dialer{Timeout: mc.cfg.Timeout}
		if cxt == nil {
			mc.netConn, err = nd.Dial(mc.cfg.Net, mc.cfg.Addr)
		} else {
			mc.netConn, err = nd.DialContext(cxt, mc.cfg.Net, mc.cfg.Addr)
		}
	}
	if err != nil {
		return err
	}

	// Enable TCP Keepalives on TCP connections
	if tc, ok := mc.netConn.(*net.TCPConn); ok {
		if err := tc.SetKeepAlive(true); err != nil {
			// Don't send COM_QUIT before handshake.
			mc.netConn.Close()
			mc.netConn = nil
			return err
		}
	}

	// Call startWatcher for context support (From Go 1.8)
	if s, ok := interface{}(mc).(watcher); ok {
		s.startWatcher()
	}

	mc.buf = newBuffer(mc.netConn)

	// Set I/O timeouts
	mc.buf.timeout = mc.cfg.ReadTimeout
	mc.writeTimeout = mc.cfg.WriteTimeout

	// Reading Handshake Initialization Packet
	cipher, err := mc.readInitPacket()
	if err != nil {
		mc.cleanup()
		return err
	}

	// Send Client Authentication Packet
	if err = mc.writeAuthPacket(cipher); err != nil {
		mc.cleanup()
		return err
	}

	// Handle response to auth packet, switch methods if possible
	if err = handleAuthResult(mc, cipher); err != nil {
		// Authentication failed and MySQL has already closed the connection
		// (https://dev.mysql.com/doc/internals/en/authentication-fails.html).
		// Do not send COM_QUIT, just cleanup and return the error.
		mc.cleanup()
		return err
	}

	if mc.cfg.MaxAllowedPacket > 0 {
		mc.maxAllowedPacket = mc.cfg.MaxAllowedPacket
	} else {
		// Get max allowed packet size
		maxap, err := mc.getSystemVar("max_allowed_packet")
		if err != nil {
			mc.Close()
			return err
		}
		mc.maxAllowedPacket = stringToInt(maxap) - 1
	}
	if mc.maxAllowedPacket < maxPacketSize {
		mc.maxWriteSize = mc.maxAllowedPacket
	}

	return err
}

//Connect opens a new connection without using a DSN
func (c MySQLConnector) Connect(cxt context.Context) (driver.Conn, error) {
	var err error

	//Validate the connection parameters
	//the following are required User,Pass,Net,Addr,DBName
	//Pass may be blank
	//The other optional parameters are not checks
	//as GO will automatically enforce proper bool types on the options
	if len(c.Cfg.User) > 32 || len(c.Cfg.User) <= 0 {
		return nil, errInvalidUser
	}

	if len(c.Cfg.Addr) <= 0 {
		return nil, errInvalidAddr
	}

	if len(c.Cfg.DBName) <= 0 {
		return nil, errInvalidDBName
	}

	if c.Cfg.Net != "tcp" {
		return nil, errInvalidNet
	}

	//New mysqlConn
	mc := &mysqlConn{
		maxAllowedPacket: maxPacketSize,
		maxWriteSize:     maxPacketSize - 1,
		closech:          make(chan struct{}),
		cfg:              c.Cfg,
		parseTime:        c.Cfg.ParseTime,
	}

	//Check if the there is a canelation before creating the connection
	select {
	case <-cxt.Done():
		return nil, cxt.Err()
	default:
		//Connect to the server and setting the connection settings
		err = connectServer(cxt, mc)
		if err != nil {
			return nil, err
		}

		return mc, nil
	}
}

//Driver returns a driver interface
func (d MySQLDriver) Driver() driver.Driver {
	return MySQLDriver{}
}

//Driver returns a driver interface
func (c MySQLConnector) Driver() driver.Driver {
	return MySQLDriver{}
}

// Open new Connection using a DSN.
// See https://github.com/go-sql-driver/mysql#dsn-data-source-name for how
// the DSN string is formated
func (d MySQLDriver) Open(dsn string) (driver.Conn, error) {
	var err error

	// New mysqlConn
	mc := &mysqlConn{
		maxAllowedPacket: maxPacketSize,
		maxWriteSize:     maxPacketSize - 1,
		closech:          make(chan struct{}),
	}
	mc.cfg, err = ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	mc.parseTime = mc.cfg.ParseTime

	err = connectServer(nil, mc)
	// Connect to Server
	if err != nil {
		return nil, err
	}

	// Handle DSN Params
	err = mc.handleParams()
	if err != nil {
		mc.Close()
		return nil, err
	}

	return mc, nil
}

func handleAuthResult(mc *mysqlConn, oldCipher []byte) error {
	// Read Result Packet
	cipher, err := mc.readResultOK()
	if err == nil {
		return nil // auth successful
	}

	if mc.cfg == nil {
		return err // auth failed and retry not possible
	}

	// Retry auth if configured to do so.
	if mc.cfg.AllowOldPasswords && err == ErrOldPassword {
		// Retry with old authentication method. Note: there are edge cases
		// where this should work but doesn't; this is currently "wontfix":
		// https://github.com/go-sql-driver/mysql/issues/184

		// If CLIENT_PLUGIN_AUTH capability is not supported, no new cipher is
		// sent and we have to keep using the cipher sent in the init packet.
		if cipher == nil {
			cipher = oldCipher
		}

		if err = mc.writeOldAuthPacket(cipher); err != nil {
			return err
		}
		_, err = mc.readResultOK()
	} else if mc.cfg.AllowCleartextPasswords && err == ErrCleartextPassword {
		// Retry with clear text password for
		// http://dev.mysql.com/doc/refman/5.7/en/cleartext-authentication-plugin.html
		// http://dev.mysql.com/doc/refman/5.7/en/pam-authentication-plugin.html
		if err = mc.writeClearAuthPacket(); err != nil {
			return err
		}
		_, err = mc.readResultOK()
	} else if mc.cfg.AllowNativePasswords && err == ErrNativePassword {
		if err = mc.writeNativeAuthPacket(cipher); err != nil {
			return err
		}
		_, err = mc.readResultOK()
	}
	return err
}

func init() {
	sql.Register("mysql", &MySQLDriver{})
}

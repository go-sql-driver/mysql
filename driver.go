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
	"database/sql"
	"database/sql/driver"
	"net"
	"sync"
)

// watcher interface is used for context support (From Go 1.8)
type watcher interface {
	startWatcher()
}

// MySQLDriver is exported to make the driver directly accessible.
// In general the driver is used via the database/sql package.
type MySQLDriver struct{}

// DialFunc is a function which can be used to establish the network connection.
// Custom dial functions must be registered with RegisterDial
type DialFunc func(addr string) (net.Conn, error)

var (
	dialsLock sync.RWMutex
	dials     map[string]DialFunc
)

// RegisterDial registers a custom dial function. It can then be used by the
// network address mynet(addr), where mynet is the registered new network.
// addr is passed as a parameter to the dial function.
func RegisterDial(net string, dial DialFunc) {
	dialsLock.Lock()
	defer dialsLock.Unlock()
	if dials == nil {
		dials = make(map[string]DialFunc)
	}
	dials[net] = dial
}

// Open new Connection.
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

	// Connect to Server
	dialsLock.RLock()
	dial, ok := dials[mc.cfg.Net]
	dialsLock.RUnlock()
	if ok {
		mc.netConn, err = dial(mc.cfg.Addr)
	} else {
		nd := net.Dialer{Timeout: mc.cfg.Timeout}
		mc.netConn, err = nd.Dial(mc.cfg.Net, mc.cfg.Addr)
	}
	if err != nil {
		return nil, err
	}

	// Enable TCP Keepalives on TCP connections
	if tc, ok := mc.netConn.(*net.TCPConn); ok {
		if err := tc.SetKeepAlive(true); err != nil {
			// Don't send COM_QUIT before handshake.
			mc.netConn.Close()
			mc.netConn = nil
			return nil, err
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
	cipher, pluginName, err := mc.readInitPacket()
	if err != nil {
		mc.cleanup()
		return nil, err
	}

	// Send Client Authentication Packet
	if err = mc.writeAuthPacket(cipher, pluginName); err != nil {
		mc.cleanup()
		return nil, err
	}

	// Handle response to auth packet, switch methods if possible
	if err = handleAuthResult(mc, cipher, pluginName); err != nil {
		// Authentication failed and MySQL has already closed the connection
		// (https://dev.mysql.com/doc/internals/en/authentication-fails.html).
		// Do not send COM_QUIT, just cleanup and return the error.
		mc.cleanup()
		return nil, err
	}

	if mc.cfg.MaxAllowedPacket > 0 {
		mc.maxAllowedPacket = mc.cfg.MaxAllowedPacket
	} else {
		// Get max allowed packet size
		maxap, err := mc.getSystemVar("max_allowed_packet")
		if err != nil {
			mc.Close()
			return nil, err
		}
		mc.maxAllowedPacket = stringToInt(maxap) - 1
	}
	if mc.maxAllowedPacket < maxPacketSize {
		mc.maxWriteSize = mc.maxAllowedPacket
	}

	// Handle DSN Params
	err = mc.handleParams()
	if err != nil {
		mc.Close()
		return nil, err
	}

	return mc, nil
}

func handleAuthResult(mc *mysqlConn, oldCipher []byte, pluginName string) error {
	// Read Result Packet
	cipher, err := mc.readResultOK()
	if err == nil {
		// handle caching_sha2_password
		// https://insidemysql.com/preparing-your-community-connector-for-mysql-8-part-2-sha256/
		if pluginName == "caching_sha2_password" {
			if len(cipher) == 1 {
				switch cipher[0] {
				case cachingSha2PasswordFastAuthSuccess:
					cipher, err = mc.readResultOK()
					if err == nil {
						return nil // auth successful
					}

				case cachingSha2PasswordPerformFullAuthentication:
					if mc.cfg.tls != nil || mc.cfg.Net == "unix" {
						if err = mc.writeClearAuthPacket(); err != nil {
							return err
						}
					} else {
						if err = mc.writePublicKeyAuthPacket(oldCipher); err != nil {
							return err
						}
					}
					cipher, err = mc.readResultOK()
					if err == nil {
						return nil // auth successful
					}

				default:
					return ErrMalformPkt
				}
			} else {
				return ErrMalformPkt
			}

		} else {
			return nil // auth successful
		}
	}

	if mc.cfg == nil {
		return err // auth failed and retry not possible
	}

	// Retry auth if configured to do so
	switch err {
	case ErrCleartextPassword:
		if mc.cfg.AllowCleartextPasswords {
			// Retry with clear text password for
			// http://dev.mysql.com/doc/refman/5.7/en/cleartext-authentication-plugin.html
			// http://dev.mysql.com/doc/refman/5.7/en/pam-authentication-plugin.html
			if err = mc.writeClearAuthPacket(); err != nil {
				return err
			}
			_, err = mc.readResultOK()
		}

	case ErrNativePassword:
		if mc.cfg.AllowNativePasswords {
			if err = mc.writeNativeAuthPacket(cipher); err != nil {
				return err
			}
			_, err = mc.readResultOK()
		}

	case ErrOldPassword:
		if mc.cfg.AllowOldPasswords {
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
		}
	}

	return err
}

func init() {
	sql.Register("mysql", &MySQLDriver{})
}

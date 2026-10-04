// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2018 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"context"
	"crypto/tls"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

const (
	authMaximumSwitch = 5
)

type connector struct {
	cfg *Config // immutable private copy.
}

func encodeConnectionAttributes(cfg *Config) string {
	connAttrsBuf := make([]byte, 0)

	// default connection attributes
	connAttrsBuf = appendLengthEncodedString(connAttrsBuf, connAttrClientName)
	connAttrsBuf = appendLengthEncodedString(connAttrsBuf, connAttrClientNameValue)
	connAttrsBuf = appendLengthEncodedString(connAttrsBuf, connAttrOS)
	connAttrsBuf = appendLengthEncodedString(connAttrsBuf, connAttrOSValue)
	connAttrsBuf = appendLengthEncodedString(connAttrsBuf, connAttrPlatform)
	connAttrsBuf = appendLengthEncodedString(connAttrsBuf, connAttrPlatformValue)
	connAttrsBuf = appendLengthEncodedString(connAttrsBuf, connAttrPid)
	connAttrsBuf = appendLengthEncodedString(connAttrsBuf, strconv.Itoa(os.Getpid()))
	serverHost, _, _ := net.SplitHostPort(cfg.Addr)
	if serverHost != "" {
		connAttrsBuf = appendLengthEncodedString(connAttrsBuf, connAttrServerHost)
		connAttrsBuf = appendLengthEncodedString(connAttrsBuf, serverHost)
	}

	// user-defined connection attributes
	for connAttr := range strings.SplitSeq(cfg.ConnectionAttributes, ",") {
		k, v, found := strings.Cut(connAttr, ":")
		if !found {
			continue
		}
		connAttrsBuf = appendLengthEncodedString(connAttrsBuf, k)
		connAttrsBuf = appendLengthEncodedString(connAttrsBuf, v)
	}

	return string(connAttrsBuf)
}

func newConnector(cfg *Config) *connector {
	return &connector{
		cfg: cfg,
	}
}

// Connect implements driver.Connector interface.
// Connect returns a connection to the database.
func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	var err error

	// Invoke beforeConnect if present, with a copy of the configuration
	cfg := c.cfg
	if c.cfg.beforeConnect != nil {
		cfg = c.cfg.Clone()
		err = c.cfg.beforeConnect(ctx, cfg)
		if err != nil {
			return nil, err
		}
		cfg.encodedAttributes = encodeConnectionAttributes(cfg)
	}

	// New mysqlConn
	mc := &mysqlConn{
		maxAllowedPacket: maxPacketSize,
		maxWriteSize:     maxPacketSize - 1,
		closech:          make(chan struct{}),
		cfg:              cfg,
	}
	mc.parseTime = mc.cfg.ParseTime

	// Connect to Server
	dctx := ctx
	if mc.cfg.Timeout > 0 {
		var cancel context.CancelFunc
		dctx, cancel = context.WithTimeout(ctx, mc.cfg.Timeout)
		defer cancel()
	}

	if mc.cfg.DialFunc != nil {
		mc.netConn, err = mc.cfg.DialFunc(dctx, mc.cfg.Net, mc.cfg.Addr)
	} else {
		dialsLock.RLock()
		dial, ok := dials[mc.cfg.Net]
		dialsLock.RUnlock()
		if ok {
			mc.netConn, err = dial(dctx, mc.cfg.Addr)
		} else {
			nd := net.Dialer{}
			mc.netConn, err = nd.DialContext(dctx, mc.cfg.Net, mc.cfg.Addr)
		}
	}
	if err != nil {
		return nil, err
	}
	mc.rawConn = mc.netConn

	// Enable TCP Keepalives on TCP connections
	if tc, ok := mc.netConn.(*net.TCPConn); ok {
		if err := tc.SetKeepAlive(true); err != nil {
			mc.cfg.Logger.Print(err)
		}
	}

	// Call startWatcher for context support (From Go 1.8)
	mc.startWatcher()
	if err := mc.watchCancel(ctx); err != nil {
		mc.cleanup()
		return nil, err
	}
	defer mc.finish()

	mc.buf = newBuffer()

	// Reading Handshake Initialization Packet
	authData, serverCapabilities, serverExtCapabilities, plugin, err := mc.readHandshakePacket()
	if err != nil {
		mc.cleanup()
		return nil, err
	}

	if plugin == "" {
		plugin = defaultAuthPlugin
	}
	if mc.cfg.TLS != nil && serverCapabilities&clientSSL == 0 && !mc.cfg.AllowFallbackToPlaintext {
		mc.cleanup()
		return nil, ErrNoTLS
	}

	// Establish TLS before starting the authentication plugin, so its context
	// describes the transport that will actually carry its responses.
	mc.initCapabilities(serverCapabilities, serverExtCapabilities)
	tlsEstablished := false
	if mc.capabilities&clientSSL != 0 {
		if err := mc.writeSSLRequestPacket(); err != nil {
			mc.cleanup()
			return nil, err
		}
		tlsConn := tls.Client(mc.netConn, mc.cfg.TLS)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			mc.cleanup()
			if cerr := mc.canceled.Value(); cerr != nil {
				return nil, cerr
			}
			return nil, err
		}
		mc.netConn = tlsConn
		tlsEstablished = true
	}

	auth := newAuthContext(mc.cfg, mc.cfg.Passwd, tlsEstablished)
	authPlugin, authResp, err := mc.initAuth(ctx, plugin, authData, auth)
	// The greeting names the server's default plugin, not necessarily the
	// connecting account's plugin. If it is unavailable or disabled, advertise
	// our default and let the server select the account's plugin with a switch.
	// Do not hide cancellation or errors from a plugin that actually ran.
	if err != nil && plugin != defaultAuthPlugin && ctx.Err() == nil && authPlugin == nil &&
		(errors.Is(err, ErrUnknownPlugin) || errors.Is(err, ErrOldPassword) || errors.Is(err, ErrCleartextPassword)) {
		mc.cfg.Logger.Print("could not use requested auth plugin '"+plugin+"': ", err.Error())
		plugin = defaultAuthPlugin
		authPlugin, authResp, err = mc.initAuth(ctx, plugin, authData, auth)
	}
	if err != nil {
		mc.cleanup()
		return nil, err
	}

	if err = mc.writeHandshakeResponsePacket(authResp, plugin); err != nil {
		mc.cleanup()
		return nil, err
	}

	// Handle response to auth packet, switch methods if possible
	if err = mc.handleAuthResult(ctx, authMaximumSwitch, authData, authPlugin, auth); err != nil {
		// Authentication failed and MySQL has already closed the connection
		// (https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase.html#sect_protocol_connection_phase_fast_path_fails).
		// Do not send COM_QUIT, just cleanup and return the error.
		mc.cleanup()
		return nil, err
	}

	// compression is enabled after auth, not right after sending handshake response.
	if mc.capabilities&clientCompress > 0 {
		mc.compress = true
		mc.compIO = newCompIO(mc)
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
		n, err := strconv.Atoi(maxap)
		if err != nil {
			mc.Close()
			return nil, fmt.Errorf("invalid max_allowed_packet value (%q): %w", maxap, err)
		}
		mc.maxAllowedPacket = n - 1
	}
	if mc.maxAllowedPacket < maxPacketSize {
		mc.maxWriteSize = mc.maxAllowedPacket
	}

	// Charset: character_set_connection, character_set_client, character_set_results
	if len(mc.cfg.charsets) > 0 {
		for _, cs := range mc.cfg.charsets {
			// ignore errors here - a charset may not exist
			if mc.cfg.Collation != "" {
				err = mc.exec("SET NAMES " + cs + " COLLATE " + mc.cfg.Collation)
			} else {
				err = mc.exec("SET NAMES " + cs)
			}
			if err == nil {
				break
			}
		}
		if err != nil {
			mc.Close()
			return nil, err
		}
	}

	// Handle DSN Params
	err = mc.handleParams()
	if err != nil {
		mc.Close()
		return nil, err
	}

	return mc, nil
}

// Driver implements driver.Connector interface.
// Driver returns &MySQLDriver{}.
func (c *connector) Driver() driver.Driver {
	return &MySQLDriver{}
}

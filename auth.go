// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2018 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"bytes"
	"crypto/rsa"
	"fmt"
	"sync"
)

// server pub keys registry
var (
	serverPubKeyLock     sync.RWMutex
	serverPubKeyRegistry map[string]*rsa.PublicKey
)

// RegisterServerPubKey registers a server RSA public key which can be used to
// send data in a secure manner to the server without receiving the public key
// in a potentially insecure way from the server first.
// Registered keys can afterwards be used adding serverPubKey=<name> to the DSN.
//
// Note: The provided rsa.PublicKey instance is exclusively owned by the driver
// after registering it and may not be modified.
//
//	data, err := os.ReadFile("mykey.pem")
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	block, _ := pem.Decode(data)
//	if block == nil || block.Type != "PUBLIC KEY" {
//		log.Fatal("failed to decode PEM block containing public key")
//	}
//
//	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	if rsaPubKey, ok := pub.(*rsa.PublicKey); ok {
//		mysql.RegisterServerPubKey("mykey", rsaPubKey)
//	} else {
//		log.Fatal("not a RSA public key")
//	}
func RegisterServerPubKey(name string, pubKey *rsa.PublicKey) {
	serverPubKeyLock.Lock()
	if serverPubKeyRegistry == nil {
		serverPubKeyRegistry = make(map[string]*rsa.PublicKey)
	}

	serverPubKeyRegistry[name] = pubKey
	serverPubKeyLock.Unlock()
}

// DeregisterServerPubKey removes the public key registered with the given name.
func DeregisterServerPubKey(name string) {
	serverPubKeyLock.Lock()
	if serverPubKeyRegistry != nil {
		delete(serverPubKeyRegistry, name)
	}
	serverPubKeyLock.Unlock()
}

func getServerPubKey(name string) (pubKey *rsa.PublicKey) {
	serverPubKeyLock.RLock()
	if v, ok := serverPubKeyRegistry[name]; ok {
		pubKey = v
	}
	serverPubKeyLock.RUnlock()
	return
}

// handleAuthResult processes the initial authentication packet and manages subsequent
// authentication flow. It reads the first authentication packet and hands off processing
// to the appropriate auth plugin.
func (mc *mysqlConn) handleAuthResult(remainingSwitch uint, initialSeed []byte, authPlugin AuthPlugin) error {
	if remainingSwitch == 0 {
		return fmt.Errorf("maximum of %d authentication switch reached", authMaximumSwitch)
	}

	data, err := mc.readPacket()
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return fmt.Errorf("%w: empty auth response packet", ErrMalformPkt)
	}

	// Loop on continuationAuth until we receive a terminal packet (OK/ERR/EOF)
	for {
		// Check for terminal packets first
		switch data[0] {
		case iOK:
			return mc.resultUnchanged().handleOkPacket(data)
		case iERR:
			return mc.handleErrorPacket(data)
		case iEOF:
			// Auth switch request
			plugin, authData := mc.parseAuthSwitchData(data, initialSeed)

			if plugin == "" {
				return fmt.Errorf("%w: malformed auth switch request", ErrMalformPkt)
			}

			newPlugin, exists := globalPluginRegistry.GetPlugin(plugin)
			if !exists {
				return fmt.Errorf("authentication plugin %q: %w", plugin, ErrUnknownPlugin)
			}

			initialAuthResponse, err := newPlugin.InitAuth(authData, mc.cfg)
			if err != nil {
				return err
			}

			if err := mc.writeAuthSwitchPacket(initialAuthResponse); err != nil {
				return err
			}

			remainingSwitch--
			if remainingSwitch == 0 {
				return fmt.Errorf("maximum of %d authentication switch reached", authMaximumSwitch)
			}

			// Continue iteratively with the new plugin and seed.
			authPlugin = newPlugin
			initialSeed = authData
			data, err = mc.readPacket()
			if err != nil {
				return err
			}
			if len(data) == 0 {
				return fmt.Errorf("%w: empty auth response packet", ErrMalformPkt)
			}
			continue
		}

		// Not a terminal packet, let the plugin process it
		// If the packet starts with 0x01 (iAuthMoreData), strip it and pass the remaining data
		// MySQL is prepending all authentication with 0x01 since 8.0,
		// MariaDB was prepending only when required (when data begins with OK/EOF/ERR),
		// and since 11.4.9, 11.8.4, always add 0x01
		pluginData := data
		if len(data) > 0 && data[0] == iAuthMoreData {
			pluginData = data[1:]
		}

		nextPacket, done, err := authPlugin.ContinuationAuth(pluginData, initialSeed, mc.cfg)
		if err != nil {
			return err
		}

		// If plugin returned a packet to send, send it and read the response
		if nextPacket != nil {
			if err := mc.writeAuthSwitchPacket(nextPacket); err != nil {
				return err
			}
			data, err = mc.readPacket()
			if err != nil {
				return err
			}
			if len(data) == 0 {
				return fmt.Errorf("%w: empty auth response packet", ErrMalformPkt)
			}
			continue
		}

		// If plugin signals done but we haven't hit a terminal packet, that's an error
		if done {
			return fmt.Errorf("%w: plugin signaled done but no terminal packet received", ErrMalformPkt)
		}

		// Plugin wants to read the next packet
		data, err = mc.readPacket()
		if err != nil {
			return err
		}
		if len(data) == 0 {
			return fmt.Errorf("%w: empty auth response packet", ErrMalformPkt)
		}
	}
}

// parseAuthSwitchData extracts the authentication plugin name and associated data
// from an authentication switch request packet.
func (mc *mysqlConn) parseAuthSwitchData(data []byte, initialSeed []byte) (string, []byte) {
	if len(data) == 1 {
		// Special case for the old authentication protocol
		return "mysql_old_password", initialSeed
	}

	pluginEndIndex := bytes.IndexByte(data, 0x00)
	if pluginEndIndex < 0 {
		return "", nil
	}

	plugin := string(data[1:pluginEndIndex])
	authData := data[pluginEndIndex+1:]
	if len(authData) > 0 && authData[len(authData)-1] == 0 {
		authData = authData[:len(authData)-1]
	}

	savedAuthData := make([]byte, len(authData))
	copy(savedAuthData, authData)
	return plugin, savedAuthData
}

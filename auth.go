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

// Hash password using pre 4.1 (old password) method
// https://github.com/atcurtis/mariadb/blob/master/mysys/my_rnd.c
type myRnd struct {
	seed1, seed2 uint32
}

const myRndMaxVal = 0x3FFFFFFF

// Pseudo random number generator
func newMyRnd(seed1, seed2 uint32) *myRnd {
	return &myRnd{
		seed1: seed1 % myRndMaxVal,
		seed2: seed2 % myRndMaxVal,
	}
}

// Tested to be equivalent to MariaDB's floating point variant
// http://play.golang.org/p/QHvhd4qved
// http://play.golang.org/p/RG0q4ElWDx
func (r *myRnd) NextByte() byte {
	r.seed1 = (r.seed1*3 + r.seed2) % myRndMaxVal
	r.seed2 = (r.seed1 + r.seed2 + 33) % myRndMaxVal

	return byte(uint64(r.seed1) * 31 / myRndMaxVal)
}

// Generate binary hash from byte string using insecure pre 4.1 method
func pwHash(password []byte) (result [2]uint32) {
	var add uint32 = 7
	var tmp uint32

	result[0] = 1345345333
	result[1] = 0x12345671

	for _, c := range password {
		// skip spaces and tabs in password
		if c == ' ' || c == '\t' {
			continue
		}

		tmp = uint32(c)
		result[0] ^= (((result[0] & 63) + add) * tmp) + (result[0] << 8)
		result[1] += (result[1] << 8) ^ result[0]
		add += tmp
	}

	// Remove sign bit (1<<31)-1)
	result[0] &= 0x7FFFFFFF
	result[1] &= 0x7FFFFFFF

	return
}

// handleAuthResult processes the initial authentication packet and manages subsequent
// authentication flow. It reads the first authentication packet and hands off processing
// to the appropriate auth plugin.
//
// Parameters:
//   - initialSeed: The initial random seed sent from server to client
//   - authPlugin: The authentication plugin to use for this connection
//
// Returns an error if authentication fails or if there's a network/protocol error.
func (mc *mysqlConn) handleAuthResult(initialSeed []byte, authPlugin AuthPlugin) error {
	data, err := mc.readPacket()
	if err != nil {
		return err
	}

	data, err = authPlugin.ProcessAuthResponse(data, initialSeed, mc)
	if err != nil {
		return err
	}

	return mc.processAuthResponse(data, initialSeed)
}

// processAuthResponse handles the different types of server responses during
// the authentication phase, routing each response type to the appropriate handler.
//
// Parameters:
//   - data: The packet data received from the server
//   - initialSeed: The initial random seed sent from server to client
//
// Returns an error if authentication fails or if there's a protocol error.
func (mc *mysqlConn) processAuthResponse(data []byte, initialSeed []byte) error {
	switch data[0] {
	case iOK:
		return mc.resultUnchanged().handleOkPacket(data)
	case iERR:
		return mc.handleErrorPacket(data)
	case iEOF:
		return mc.handleAuthSwitch(data, initialSeed)
	default:
		return ErrMalformPkt
	}
}

// handleAuthSwitch processes an authentication plugin switch request from the server.
// This happens when the server wants to use a different authentication method than
// what was initially negotiated.
//
// Parameters:
//   - data: The packet data received from the server containing switch request information
//   - initialSeed: The initial random seed from the server
//
// Returns an error if the requested plugin is not supported, or if there's an error
// during the authentication process.
func (mc *mysqlConn) handleAuthSwitch(data []byte, initialSeed []byte) error {
	plugin, authData := mc.parseAuthSwitchData(data, initialSeed)

	authPlugin, exists := globalPluginRegistry.GetPlugin(plugin)
	if !exists {
		return fmt.Errorf("this authentication plugin '%s' is not supported", plugin)
	}

	cachedEncryptPassword, err := authPlugin.InitAuth(authData, mc.cfg)
	if err != nil {
		return err
	}

	if err := mc.writeAuthSwitchPacket(cachedEncryptPassword); err != nil {
		return err
	}

	data, err = mc.readPacket()
	if err != nil {
		return err
	}

	switch data[0] {
	case iERR, iOK, iEOF:
		return mc.processAuthResponse(data, initialSeed)
	default:
		data, err = authPlugin.ProcessAuthResponse(data, authData, mc)
		if err != nil {
			return err
		}
		return mc.processAuthResponse(data, initialSeed)
	}
}

// parseAuthSwitchData extracts the authentication plugin name and associated data
// from an authentication switch request packet.
//
// Parameters:
//   - data: The packet data from an authentication switch request
//   - initialSeed: The initial seed, used as fallback for old authentication method
//
// Returns:
//   - string: The name of the requested authentication plugin
//   - []byte: The authentication data to be used with the plugin
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

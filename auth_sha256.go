// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2023 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/x509"
	"encoding/pem"
	"fmt"
)

// Sha256PasswordPlugin implements the sha256_password authentication
// This plugin provides secure password-based authentication using SHA256 and RSA encryption.
type Sha256PasswordPlugin struct{ AuthPlugin }

func init() {
	RegisterAuthPlugin(&Sha256PasswordPlugin{})
}

func (p *Sha256PasswordPlugin) PluginName() string {
	return "sha256_password"
}

// InitAuth initializes the authentication process.
//
// The function follows these rules:
// 1. If no password is configured, sends a single byte indicating empty password
// 2. If TLS is enabled, sends the password in cleartext
// 3. If a public key is available, encrypts the password and sends it
// 4. Otherwise, requests the server's public key
func (p *Sha256PasswordPlugin) InitAuth(authData []byte, cfg *Config) ([]byte, error) {
	if len(cfg.Passwd) == 0 {
		return []byte{0}, nil
	}

	// Unlike caching_sha2_password, sha256_password does not accept
	// cleartext password on unix transport.
	if cfg.TLS != nil {
		// Write cleartext auth packet
		return append([]byte(cfg.Passwd), 0), nil
	}

	if cfg.pubKey == nil {
		// Request public key from server
		return []byte{1}, nil
	}

	// Encrypt password using the public key
	enc, err := encryptPassword(cfg.Passwd, authData, cfg.pubKey)
	if err != nil {
		return nil, fmt.Errorf("failed to encrypt password: %w", err)
	}
	return enc, nil
}

// ContinuationAuth processes the server's response to our authentication attempt.
//
// The server can respond in three ways:
// 1. OK packet - Authentication successful
// 2. Error packet - Authentication failed
// 3. More data packet - Contains the server's public key for password encryption
func (p *Sha256PasswordPlugin) continuationAuth(packet []byte, authData []byte, mc *mysqlConn) ([]byte, error) {
	if len(packet) == 0 {
		return nil, fmt.Errorf("%w: empty auth response packet", ErrMalformPkt)
	}

	switch packet[0] {
	case iOK, iERR, iEOF:
		return packet, nil

	case iAuthMoreData:
		// Parse public key from PEM format
		block, rest := pem.Decode(packet[1:])
		if block == nil {
			return nil, fmt.Errorf("invalid PEM data in auth response: %q", rest)
		}

		// Parse the public key
		pub, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("failed to parse public key: %w", err)
		}

		pubKey, ok := pub.(*rsa.PublicKey)
		if !ok {
			return nil, fmt.Errorf("server sent an invalid public key type: %T", pub)
		}

		// Send encrypted password
		enc, err := encryptPassword(mc.cfg.Passwd, authData, pubKey)
		if err != nil {
			return nil, fmt.Errorf("failed to encrypt password with server key: %w", err)
		}

		// Send the encrypted password
		if err = mc.writeAuthSwitchPacket(enc); err != nil {
			return nil, fmt.Errorf("failed to send encrypted password: %w", err)
		}

		return mc.readPacket()

	default:
		return nil, fmt.Errorf("%w: unexpected packet type %d", ErrMalformPkt, packet[0])
	}
}

// encryptPassword encrypts the password using RSA-OAEP with SHA1 hash.
//
// The process:
// 1. XORs the password with the auth seed to prevent replay attacks
// 2. Encrypts the XORed password using RSA-OAEP with SHA1
//
// The encryption uses OAEP padding which is more secure than PKCS#1 v1.5 padding.
func encryptPassword(password string, seed []byte, pub *rsa.PublicKey) ([]byte, error) {
	if pub == nil {
		return nil, fmt.Errorf("public key is nil")
	}

	// Create the plaintext by XORing password with seed
	plain := make([]byte, len(password)+1)
	copy(plain, password)
	for i := range plain {
		j := i % len(seed)
		plain[i] ^= seed[j]
	}

	// Encrypt using RSA-OAEP with SHA1
	sha1Hash := sha1.New()
	return rsa.EncryptOAEP(sha1Hash, rand.Reader, pub, plain, nil)
}

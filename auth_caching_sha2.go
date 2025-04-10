// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2023 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"fmt"
)

// CachingSha2PasswordPlugin implements the caching_sha2_password authentication
// This plugin provides secure password-based authentication using SHA256 and RSA encryption,
// with server-side caching of password verifiers for improved performance.
type CachingSha2PasswordPlugin struct {
	AuthPlugin
}

func init() {
	RegisterAuthPlugin(&CachingSha2PasswordPlugin{})
}

func (p *CachingSha2PasswordPlugin) GetPluginName() string {
	return "caching_sha2_password"
}

// InitAuth initializes the authentication process by scrambling the password.
//
// The scrambling process uses a three-step SHA256 hash:
// 1. SHA256(password)
// 2. SHA256(SHA256(password))
// 3. XOR(SHA256(password), SHA256(SHA256(SHA256(password)), scramble))
func (p *CachingSha2PasswordPlugin) InitAuth(authData []byte, cfg *Config) ([]byte, error) {
	return scrambleSHA256Password(authData, cfg.Passwd), nil
}

// ProcessAuthResponse processes the server's response to our authentication attempt.
//
// The authentication flow can take several paths:
//  1. Fast auth success (password found in cache)
//  2. Full authentication needed:
//     a. With TLS: send cleartext password
//     b. Without TLS:
//     - Request server's public key if not cached
//     - Encrypt password with RSA public key
//     - Send encrypted password
func (p *CachingSha2PasswordPlugin) ProcessAuthResponse(packet []byte, authData []byte, mc *mysqlConn) ([]byte, error) {

	if len(packet) == 0 {
		return nil, fmt.Errorf("%w: empty auth response packet", ErrMalformPkt)
	}

	switch packet[0] {
	case iOK, iERR, iEOF:
		return packet, nil
	case iAuthMoreData:
		switch len(packet) {
		case 1:
			return mc.readPacket() // Auth successful

		case 2:
			switch packet[1] {
			case 3:
				// the password was found in the server's cache
				return mc.readPacket()

			case 4:
				// indicates full authentication is needed
				// For TLS connections or Unix socket, send cleartext password
				if mc.cfg.TLS != nil || mc.cfg.Net == "unix" {
					err := mc.writeAuthSwitchPacket(append([]byte(mc.cfg.Passwd), 0))
					if err != nil {
						return nil, fmt.Errorf("failed to send cleartext password: %w", err)
					}
				} else {
					// For non-TLS connections, use RSA encryption
					pubKey := mc.cfg.pubKey
					if pubKey == nil {
						// Request public key from server
						packet, err := mc.buf.takeSmallBuffer(4 + 1)
						if err != nil {
							return nil, fmt.Errorf("failed to allocate buffer: %w", err)
						}
						packet[4] = 2
						if err = mc.writePacket(packet); err != nil {
							return nil, fmt.Errorf("failed to request public key: %w", err)
						}

						// Read public key packet
						if packet, err = mc.readPacket(); err != nil {
							return nil, fmt.Errorf("failed to read public key: %w", err)
						}

						if packet[0] != iAuthMoreData {
							return nil, fmt.Errorf("unexpected packet type %d when requesting public key", packet[0])
						}

						// Parse public key from PEM format
						block, rest := pem.Decode(packet[1:])
						if block == nil {
							return nil, fmt.Errorf("invalid PEM data in auth response: %q", rest)
						}

						// Parse the public key
						pkix, err := x509.ParsePKIXPublicKey(block.Bytes)
						if err != nil {
							return nil, fmt.Errorf("failed to parse public key: %w", err)
						}
						pubKey = pkix.(*rsa.PublicKey)
					}

					// Encrypt and send password
					enc, err := encryptPassword(mc.cfg.Passwd, authData, pubKey)
					if err != nil {
						return nil, fmt.Errorf("failed to encrypt password: %w", err)
					}
					if err = mc.writeAuthSwitchPacket(enc); err != nil {
						return nil, fmt.Errorf("failed to send encrypted password: %w", err)
					}
				}
				return mc.readPacket()

			default:
				return nil, fmt.Errorf("%w: unknown auth state %d", ErrMalformPkt, packet[1])
			}

		default:
			return nil, fmt.Errorf("%w: unexpected packet length %d", ErrMalformPkt, len(packet))
		}
	default:
		return nil, fmt.Errorf("%w: expected auth more data packet", ErrMalformPkt)
	}
}

// scrambleSHA256Password implements MySQL 8+ password scrambling.
//
// The algorithm is:
// 1. SHA256(password)
// 2. SHA256(SHA256(SHA256(password)))
// 3. XOR(SHA256(password), SHA256(SHA256(SHA256(password)), scramble))
//
// This provides a way to verify the password without storing it in cleartext.
func scrambleSHA256Password(scramble []byte, password string) []byte {
	if len(password) == 0 {
		return []byte{}
	}

	// First hash: SHA256(password)
	crypt := sha256.New()
	crypt.Write([]byte(password))
	message1 := crypt.Sum(nil)

	// Second hash: SHA256(SHA256(password))
	crypt.Reset()
	crypt.Write(message1)
	message1Hash := crypt.Sum(nil)

	// Third hash: SHA256(SHA256(SHA256(password)), scramble)
	crypt.Reset()
	crypt.Write(message1Hash)
	crypt.Write(scramble)
	message2 := crypt.Sum(nil)

	// XOR the first hash with the third hash
	for i := range message1 {
		message1[i] ^= message2[i]
	}

	return message1
}

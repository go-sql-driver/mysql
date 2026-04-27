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

// Authentication response constants
const (
	cachingSha2RequestPublicKey = 2 // Request server public key for RSA encryption
	cachingSha2FastAuth         = 3 // Password found in cache
	cachingSha2FullAuthNeeded   = 4 // Full authentication needed
)

// CachingSha2PasswordPlugin implements the caching_sha2_password authentication
// This plugin provides secure password-based authentication using SHA256 and RSA encryption,
// with server-side caching of password verifiers for improved performance.
type CachingSha2PasswordPlugin struct {
	AuthPlugin
}

func init() {
	RegisterAuthPlugin(func() AuthPlugin { return &CachingSha2PasswordPlugin{} })
}

func (p *CachingSha2PasswordPlugin) PluginName() string {
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

// ContinuationAuth processes the server's response to our authentication attempt.
//
// The authentication flow can take several paths:
//  1. Fast auth success (password found in cache)
//  2. Full authentication needed:
//     a. With TLS: send cleartext password
//     b. Without TLS:
//     - Request server's public key if not cached
//     - Encrypt password with RSA public key
//     - Send encrypted password
func (p *CachingSha2PasswordPlugin) ContinuationAuth(packet []byte, authData []byte, cfg *Config) ([]byte, bool, error) {
	// Driver already checked for OK/ERR/EOF and stripped 0x01 continuation byte
	// So we receive the payload directly

	if len(packet) == 0 {
		// Empty packet after stripping 0x01 means auth successful, need to read next packet
		return nil, false, nil
	}

	if len(packet) == 1 {
		switch packet[0] {
		case cachingSha2FastAuth:
			// the password was found in the server's cache
			// Need to read next packet
			return nil, false, nil

		case cachingSha2FullAuthNeeded:
			// indicates full authentication is needed
			// For TLS connections or Unix socket, send cleartext password
			if cfg.TLS != nil || cfg.Net == "unix" {
				return append([]byte(cfg.Passwd), 0), false, nil
			}

			// For non-TLS connections, use RSA encryption
			pubKey := cfg.pubKey
			if pubKey == nil {
				// Request public key from server
				return []byte{cachingSha2RequestPublicKey}, false, nil
			}

			// Encrypt and send password
			enc, err := encryptPassword(cfg.Passwd, authData, pubKey)
			if err != nil {
				return nil, false, fmt.Errorf("failed to encrypt password: %w", err)
			}
			return enc, false, nil

		default:
			return nil, false, fmt.Errorf("%w: unknown auth state %d", ErrMalformPkt, packet[0])
		}
	}

	// This might be a public key response (PEM data)
	// Parse public key from PEM format
	block, rest := pem.Decode(packet)
	if block == nil {
		return nil, false, fmt.Errorf("invalid PEM data in auth response: %q", rest)
	}

	// Parse the public key
	pkix, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, false, fmt.Errorf("failed to parse public key: %w", err)
	}

	pubKey, ok := pkix.(*rsa.PublicKey)
	if !ok {
		return nil, false, fmt.Errorf("server sent an invalid public key type: %T", pkix)
	}

	// Encrypt and send password
	enc, err := encryptPassword(cfg.Passwd, authData, pubKey)
	if err != nil {
		return nil, false, fmt.Errorf("failed to encrypt password: %w", err)
	}
	return enc, false, nil
}

// scrambleSHA256Password implements MySQL 8+ password scrambling.
//
// The algorithm is:
// 1. SHA256(password)
// 2. SHA256(SHA256(password))
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

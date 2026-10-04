// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2023 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"context"
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

// cachingSha2PasswordPlugin implements the caching_sha2_password authentication
// This plugin provides secure password-based authentication using SHA256 and RSA encryption,
// with server-side caching of password verifiers for improved performance.
type cachingSha2PasswordPlugin struct {
	state cachingSha2State
}

type cachingSha2State uint8

const (
	cachingSha2Initial cachingSha2State = iota
	cachingSha2PublicKey
	cachingSha2Result
)

// Compile-time assertion that cachingSha2PasswordPlugin implements AuthPlugin.
var _ AuthPlugin = (*cachingSha2PasswordPlugin)(nil)

func init() {
	RegisterAuthPlugin("caching_sha2_password", func() AuthPlugin { return &cachingSha2PasswordPlugin{} })
}

// InitAuth initializes the authentication process by scrambling the password.
//
// The scrambling process uses a three-step SHA256 hash:
// 1. SHA256(password)
// 2. SHA256(SHA256(password))
// 3. XOR(SHA256(password), SHA256(SHA256(SHA256(password)), scramble))
func (p *cachingSha2PasswordPlugin) InitAuth(ctx context.Context, authData []byte, auth *AuthContext) ([]byte, error) {
	p.state = cachingSha2Initial
	return scrambleSHA256Password(authData, auth.Password()), nil
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
func (p *cachingSha2PasswordPlugin) ContinuationAuth(ctx context.Context, packet, authData []byte, auth *AuthContext) ([]byte, error) {
	// Driver already checked for OK/ERR/EOF and stripped 0x01 continuation byte
	// So we receive the payload directly

	switch p.state {
	case cachingSha2Initial:
		if len(packet) != 1 {
			return nil, ErrMalformPkt
		}
		// After fast authentication or sending the password, only a driver-
		// handled OK, ERR, or auth switch is valid. A public key is accepted
		// only after we explicitly request one below.
		p.state = cachingSha2Result
		switch packet[0] {
		case cachingSha2FastAuth:
			// the password was found in the server's cache
			// Need to read next packet
			return nil, nil

		case cachingSha2FullAuthNeeded:
			// indicates full authentication is needed
			// For TLS connections or Unix socket, send cleartext password
			if auth.TLS() || auth.UnixSocket() {
				return append([]byte(auth.Password()), 0), nil
			}

			// For non-TLS connections, use RSA encryption
			pubKey := auth.ServerPublicKey()
			if pubKey == nil {
				// Request public key from server
				p.state = cachingSha2PublicKey
				return []byte{cachingSha2RequestPublicKey}, nil
			}

			// Encrypt and send password
			enc, err := encryptPassword(auth.Password(), authData, pubKey)
			if err != nil {
				return nil, fmt.Errorf("failed to encrypt password: %w", err)
			}
			return enc, nil

		default:
			return nil, fmt.Errorf("%w: unknown auth state %d", ErrMalformPkt, packet[0])
		}
	case cachingSha2PublicKey:
		p.state = cachingSha2Result
	default:
		return nil, ErrMalformPkt
	}

	// Parse the public key we requested. Never replace a configured key with
	// an unsolicited key supplied by the peer.
	block, _ := pem.Decode(packet)
	if block == nil {
		return nil, fmt.Errorf("%w: invalid PEM data in auth response", ErrMalformPkt)
	}
	if block.Type != "PUBLIC KEY" {
		return nil, fmt.Errorf("%w: unexpected PEM block type %q in auth response", ErrMalformPkt, block.Type)
	}

	// Parse the public key
	pkix, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse public key: %w", err)
	}

	pubKey, ok := pkix.(*rsa.PublicKey)
	if !ok {
		return nil, fmt.Errorf("server sent an invalid public key type: %T", pkix)
	}

	// Encrypt and send password
	enc, err := encryptPassword(auth.Password(), authData, pubKey)
	if err != nil {
		return nil, fmt.Errorf("failed to encrypt password: %w", err)
	}
	return enc, nil
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

// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2026 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"context"
	"crypto/ed25519"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha512"
	"fmt"
)

// parsecMaxIterationFactor bounds the PBKDF2 cost a server may request.
// The number of rounds is 1024 << factor, so 10 allows up to 1,048,576 rounds.
const parsecMaxIterationFactor = 10

// parsecPlugin implements the MariaDB parsec authentication.
//
// The server sends a 32-byte nonce. The client requests the extended salt
// with an empty packet, derives an Ed25519 key from the password with
// PBKDF2-HMAC-SHA512, and signs the server nonce followed by a client nonce.
//
// See: https://mariadb.com/kb/en/connection/#parsec-plugin
type parsecPlugin struct {
	saltRequested bool
}

func init() {
	RegisterAuthPlugin("parsec", func() AuthPlugin { return &parsecPlugin{} })
}

func (p *parsecPlugin) InitAuth(ctx context.Context, authData []byte, auth *AuthContext) ([]byte, error) {
	if len(authData) != 32 {
		return nil, ErrMalformPkt
	}
	p.saltRequested = true
	// An empty packet requests the extended salt.
	return []byte{}, nil
}

// ContinuationAuth processes the extended salt:
//
//	string<1>  'P' (KDF algorithm = PBKDF2)
//	int<1>     iteration factor, rounds = 1024 << factor
//	byte<EOF>  salt
//
// and responds with the 32-byte client nonce followed by the 64-byte signature.
func (p *parsecPlugin) ContinuationAuth(ctx context.Context, packet, authData []byte, auth *AuthContext) ([]byte, error) {
	if !p.saltRequested {
		return nil, ErrMalformPkt
	}
	p.saltRequested = false

	if len(packet) < 3 || packet[0] != 'P' {
		return nil, fmt.Errorf("%w: unexpected parsec extended salt", ErrMalformPkt)
	}
	factor := int(packet[1])
	if factor > parsecMaxIterationFactor {
		return nil, fmt.Errorf("parsec: server requested iteration factor %d, maximum is %d", factor, parsecMaxIterationFactor)
	}
	salt := packet[2:]

	seed, err := pbkdf2.Key(sha512.New, auth.Password(), salt, 1024<<factor, ed25519.SeedSize)
	if err != nil {
		return nil, err
	}
	priv := ed25519.NewKeyFromSeed(seed)

	resp := make([]byte, 32+ed25519.SignatureSize)
	if _, err := rand.Read(resp[:32]); err != nil {
		return nil, err
	}
	// sign server nonce + client nonce
	msg := make([]byte, 0, len(authData)+32)
	msg = append(msg, authData...)
	msg = append(msg, resp[:32]...)
	copy(resp[32:], ed25519.Sign(priv, msg))
	return resp, nil
}

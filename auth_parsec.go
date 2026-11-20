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

const (
	parsecNonceSize = 32
	parsecSaltSize  = 18
	// MariaDB accepts factors 0 through 3 in stored PARSEC credentials.
	// Check the factor before deriving the key to bound the work per exchange.
	parsecMaxIterationFactor = 3
)

// parsecPlugin implements MariaDB PARSEC authentication.
// https://mariadb.com/docs/server/reference/clientserver-protocol/1-connecting/connection#parsec-plugin
type parsecPlugin struct {
	saltRequested bool
}

func init() {
	RegisterAuthPlugin("parsec", func() AuthPlugin { return &parsecPlugin{} })
}

func (p *parsecPlugin) InitAuth(ctx context.Context, authData []byte, auth *AuthContext) ([]byte, error) {
	if len(authData) != parsecNonceSize {
		return nil, ErrMalformPkt
	}
	p.saltRequested = true
	// An empty packet requests the extended salt.
	return []byte{}, nil
}

// ContinuationAuth receives the KDF identifier, iteration factor and 18-byte salt.
// The client signs the server nonce followed by a fresh client nonce.
func (p *parsecPlugin) ContinuationAuth(ctx context.Context, packet, authData []byte, auth *AuthContext) ([]byte, error) {
	if !p.saltRequested {
		return nil, ErrMalformPkt
	}
	p.saltRequested = false

	if len(packet) != 2+parsecSaltSize || packet[0] != 'P' {
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

	var message [2 * parsecNonceSize]byte
	copy(message[:parsecNonceSize], authData)
	clientNonce := message[parsecNonceSize:]
	rand.Read(clientNonce)
	return append(clientNonce, ed25519.Sign(priv, message[:])...), nil
}

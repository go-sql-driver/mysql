// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2023 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha512"
	"fmt"

	"golang.org/x/crypto/pbkdf2"
)

// ParsecPlugin implements the parsec authentication
type ParsecPlugin struct{ AuthPlugin }

func init() {
	RegisterAuthPlugin(&ParsecPlugin{})
}

func (p *ParsecPlugin) GetPluginName() string {
	return "parsec"
}

func (p *ParsecPlugin) InitAuth(authData []byte, cfg *Config) ([]byte, error) {
	return []byte{}, nil
}

func (p *ParsecPlugin) processParsecExtSalt(extSalt, serverScramble []byte, password string) ([]byte, error) {
	return ProcessParsecExtSalt(extSalt, serverScramble, password)
}

func (p *ParsecPlugin) ProcessAuthResponse(packet []byte, authData []byte, mc *mysqlConn) ([]byte, error) {
	// Process the ext-salt and generate the client nonce and signature
	authResp, err := p.processParsecExtSalt(packet, authData, mc.cfg.Passwd)
	if err != nil {
		return nil, fmt.Errorf("parsec auth failed: %w", err)
	}

	// Send the authentication response
	if err = mc.writeAuthSwitchPacket(authResp); err != nil {
		return nil, fmt.Errorf("failed to write auth switch packet: %w", err)
	}

	// Read the final authentication result
	return mc.readPacket()
}

// ProcessParsecExtSalt processes the ext-salt received from the server and generates
// the authentication response for PARSEC authentication.
//
// The ext-salt format is: 'P' + iteration factor + salt
// The iteration count is 1024 << iteration factor (0x0 means 1024, 0x1 means 2048, etc.)
//
// The authentication process:
// 1. Validates the ext-salt format and iteration factor
// 2. Generates a random 32-byte client nonce
// 3. Derives a key using PBKDF2-HMAC-SHA512 with the password and salt
// 4. Uses the derived key as an Ed25519 seed to generate a signing key
// 5. Signs the concatenation of server scramble and client nonce
// 6. Returns the concatenation of client nonce and signature
//
// This function is exported for testing purposes
func ProcessParsecExtSalt(extSalt, serverScramble []byte, password string) ([]byte, error) {
	// Validate ext-salt format and length
	if len(extSalt) < 3 {
		return nil, fmt.Errorf("%w: ext-salt too short", ErrParsecAuth)
	}
	if extSalt[0] != 'P' {
		return nil, fmt.Errorf("%w: invalid ext-salt prefix", ErrParsecAuth)
	}

	// Parse and validate iteration factor
	iterationFactor := int(extSalt[1])
	if iterationFactor < 0 || iterationFactor > 3 {
		return nil, fmt.Errorf("%w: invalid iteration factor", ErrParsecAuth)
	}

	// Calculate iterations
	iterations := 1024 << iterationFactor

	// Extract the salt (everything after prefix and iteration factor)
	salt := extSalt[2:]
	if len(salt) == 0 {
		return nil, fmt.Errorf("%w: empty salt", ErrParsecAuth)
	}

	// Generate a random 32-byte client nonce
	clientNonce := make([]byte, 32)
	if _, err := rand.Read(clientNonce); err != nil {
		return nil, fmt.Errorf("failed to generate client nonce: %w", err)
	}

	// Generate the PBKDF2 key using SHA-512 and the configured iterations
	derivedKey := pbkdf2.Key([]byte(password), salt, iterations, ed25519.SeedSize, sha512.New)

	// Create message to sign (server scramble + client nonce)
	message := make([]byte, len(serverScramble)+len(clientNonce))
	copy(message, serverScramble)
	copy(message[len(serverScramble):], clientNonce)

	// Generate Ed25519 private key from derived key
	privateKey := ed25519.NewKeyFromSeed(derivedKey[:ed25519.SeedSize])

	// Sign the message
	signature := ed25519.Sign(privateKey, message)

	// Prepare the authentication response: client nonce (32 bytes) + signature (64 bytes)
	response := make([]byte, len(clientNonce)+len(signature))
	copy(response, clientNonce)
	copy(response[len(clientNonce):], signature)

	return response, nil
}

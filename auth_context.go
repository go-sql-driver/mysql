// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2026 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"crypto/rsa"
	"math/big"
)

// AuthContext contains the credentials and transport information for the current
// authentication exchange. The driver creates a new snapshot whenever it starts
// a plugin and does not change it during that exchange. Plugins must not modify it.
type AuthContext struct {
	user            string
	password        string
	tls             bool
	unixSocket      bool
	serverPublicKey *rsa.PublicKey
}

// newAuthContext snapshots the selected password and established transport.
// tlsEstablished describes the completed MySQL TLS handshake, not cfg.TLS.
func newAuthContext(cfg *Config, password string, tlsEstablished bool) *AuthContext {
	auth := &AuthContext{
		user:            cfg.User,
		password:        password,
		tls:             tlsEstablished,
		unixSocket:      cfg.Net == "unix",
		serverPublicKey: cfg.pubKey,
	}
	auth.serverPublicKey = auth.ServerPublicKey()
	return auth
}

// User returns the username supplied for the connection.
func (a *AuthContext) User() string { return a.user }

// Password returns the password selected for this authentication exchange.
func (a *AuthContext) Password() string { return a.password }

// TLS reports whether the driver has completed the MySQL TLS handshake.
// It does not indicate whether the server's certificate was verified, nor does
// it describe encryption provided by a custom dialer or an external tunnel.
func (a *AuthContext) TLS() bool { return a.tls }

// UnixSocket reports whether the connection uses the configured unix network.
func (a *AuthContext) UnixSocket() bool { return a.unixSocket }

// ServerPublicKey returns a copy of the configured server RSA public key,
// or nil if none was configured. Modifying the copy does not affect the driver
// or subsequent calls to ServerPublicKey.
func (a *AuthContext) ServerPublicKey() *rsa.PublicKey {
	if a.serverPublicKey == nil {
		return nil
	}
	key := *a.serverPublicKey
	if key.N != nil {
		key.N = new(big.Int).Set(key.N)
	}
	return &key
}

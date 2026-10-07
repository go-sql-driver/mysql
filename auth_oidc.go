// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2026 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import "context"

// OIDC is selected explicitly by OIDCToken, not by the server or plugin registry.
type openIDConnectAuthPlugin struct {
	simpleAuth
	token string
}

func (p *openIDConnectAuthPlugin) InitAuth(ctx context.Context, _ []byte, auth *AuthContext) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !auth.TLS() {
		return nil, ErrOpenIDConnectTLS
	}
	// Construct the bearer-token response only after the TLS handshake and
	// the application's verification policy have succeeded.
	return appendLengthEncodedString([]byte{1}, p.token), nil
}

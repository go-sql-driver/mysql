// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2023 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import "context"

// ClearPasswordPlugin implements the mysql_clear_password authentication.
//
// This plugin sends passwords in cleartext. The driver requires
// AllowCleartextPasswords to be enabled before starting it. Applications are
// responsible for protecting the transport, for example with TLS or a tunnel.
//
// See: http://dev.mysql.com/doc/refman/5.7/en/cleartext-authentication-plugin.html
//
//	http://dev.mysql.com/doc/refman/5.7/en/pam-authentication-plugin.html
type ClearPasswordPlugin struct {
	SimpleAuth
}

func init() {
	RegisterAuthPlugin(func() AuthPlugin { return &ClearPasswordPlugin{} })
}

func (p *ClearPasswordPlugin) PluginName() string {
	return "mysql_clear_password"
}

// InitAuth implements the cleartext password authentication.
// The driver checks AllowCleartextPasswords before calling this method.
//
// The cleartext password is sent as a null-terminated string.
// This is required by the server to support external authentication
// systems that need access to the original password.
func (p *ClearPasswordPlugin) InitAuth(ctx context.Context, authData []byte, auth *AuthContext) ([]byte, error) {
	// Send password as null-terminated string
	return append([]byte(auth.Password()), 0), nil
}

// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2023 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

// ClearPasswordPlugin implements the mysql_clear_password authentication.
//
// This plugin sends passwords in cleartext and should only be used:
// 1. Over TLS/SSL connections
// 2. Over Unix domain sockets
// 3. When required by authentication methods like PAM
//
// See: http://dev.mysql.com/doc/refman/5.7/en/cleartext-authentication-plugin.html
//
//	http://dev.mysql.com/doc/refman/5.7/en/pam-authentication-plugin.html
type ClearPasswordPlugin struct {
	AuthPlugin
}

func init() {
	RegisterAuthPlugin(&ClearPasswordPlugin{})
}

func (p *ClearPasswordPlugin) GetPluginName() string {
	return "mysql_clear_password"
}

// InitAuth implements the cleartext password authentication.
// It will return an error if AllowCleartextPasswords is false.
//
// The cleartext password is sent as a null-terminated string.
// This is required by the server to support external authentication
// systems that need access to the original password.
func (p *ClearPasswordPlugin) InitAuth(authData []byte, cfg *Config) ([]byte, error) {
	if !cfg.AllowCleartextPasswords {
		return nil, ErrCleartextPassword
	}

	// Send password as null-terminated string
	return append([]byte(cfg.Passwd), 0), nil
}

// ProcessAuthResponse handles the server's response to our authentication attempt.
// For cleartext authentication, we simply return the packet as is since no
// additional processing is needed.
func (p *ClearPasswordPlugin) ProcessAuthResponse(packet []byte, authData []byte, mc *mysqlConn) ([]byte, error) {
	return packet, nil
}

// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2023 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

// OldPasswordPlugin implements the mysql_old_password authentication
type OldPasswordPlugin struct{ AuthPlugin }

func init() {
	RegisterAuthPlugin(&OldPasswordPlugin{})
}

func (p *OldPasswordPlugin) GetPluginName() string {
	return "mysql_old_password"
}

func (p *OldPasswordPlugin) InitAuth(authData []byte, cfg *Config) ([]byte, error) {
	if !cfg.AllowOldPasswords {
		return nil, ErrOldPassword
	}
	if cfg.Passwd == "" {
		return nil, nil
	}
	// Note: there are edge cases where this should work but doesn't;
	// this is currently "wontfix":
	// https://github.com/go-sql-driver/mysql/issues/184
	return append(p.scrambleOldPassword(authData[:8], cfg.Passwd), 0), nil
}

func (p *OldPasswordPlugin) ProcessAuthResponse(packet []byte, authData []byte, mc *mysqlConn) ([]byte, error) {
	return packet, nil
}

// Hash password using insecure pre 4.1 method
func (p *OldPasswordPlugin) scrambleOldPassword(scramble []byte, password string) []byte {
	scramble = scramble[:8]

	hashPw := pwHash([]byte(password))
	hashSc := pwHash(scramble)

	r := newMyRnd(hashPw[0]^hashSc[0], hashPw[1]^hashSc[1])

	var out [8]byte
	for i := range out {
		out[i] = r.NextByte() + 64
	}

	mask := r.NextByte()
	for i := range out {
		out[i] ^= mask
	}

	return out[:]
}

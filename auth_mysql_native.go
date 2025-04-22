// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2023 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import "crypto/sha1"

// NativePasswordPlugin implements the mysql_native_password authentication
type NativePasswordPlugin struct {
	SimpleAuth
}

func init() {
	RegisterAuthPlugin(&NativePasswordPlugin{})
}

func (p *NativePasswordPlugin) PluginName() string {
	return "mysql_native_password"
}

func (p *NativePasswordPlugin) InitAuth(authData []byte, cfg *Config) ([]byte, error) {
	if !cfg.AllowNativePasswords {
		return nil, ErrNativePassword
	}
	if cfg.Passwd == "" {
		return nil, nil
	}
	return p.scramblePassword(authData[:20], cfg.Passwd), nil
}

// Hash password using 4.1+ method (SHA1)
func (p *NativePasswordPlugin) scramblePassword(scramble []byte, password string) []byte {
	if len(password) == 0 {
		return nil
	}

	// stage1Hash = SHA1(password)
	crypt := sha1.New()
	crypt.Write([]byte(password))
	stage1 := crypt.Sum(nil)

	// scrambleHash = SHA1(scramble + SHA1(stage1Hash))
	// inner Hash
	crypt.Reset()
	crypt.Write(stage1)
	hash := crypt.Sum(nil)

	// outer Hash
	crypt.Reset()
	crypt.Write(scramble)
	crypt.Write(hash)
	scramble = crypt.Sum(nil)

	// token = scrambleHash XOR stage1Hash
	for i := range scramble {
		scramble[i] ^= stage1[i]
	}
	return scramble
}

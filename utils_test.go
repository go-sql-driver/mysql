// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2013 Julien Schmidt. All rights reserved.
// http://www.julienschmidt.com
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"fmt"
	"testing"
)

var testDSNs = []struct {
	in  string
	out string
}{
	{"username:password@protocol(address)/dbname?param=value", "&{user:username passwd:password net:protocol addr:address dbname:dbname params:map[param:value]}"},
	{"user@unix(/path/to/socket)/dbname?charset=utf8", "&{user:user passwd: net:unix addr:/path/to/socket dbname:dbname params:map[charset:utf8]}"},
	{"user:password@tcp(localhost:5555)/dbname?charset=utf8", "&{user:user passwd:password net:tcp addr:localhost:5555 dbname:dbname params:map[charset:utf8]}"},
	{"user:password@tcp(localhost:5555)/dbname?charset=utf8mb4,utf8", "&{user:user passwd:password net:tcp addr:localhost:5555 dbname:dbname params:map[charset:utf8mb4,utf8]}"},
	{"user:password@/dbname", "&{user:user passwd:password net:tcp addr:127.0.0.1:3306 dbname:dbname params:map[]}"},
	{"user:p@ss(word)@tcp([de:ad:be:ef::ca:fe]:80)/dbname", "&{user:user passwd:p@ss(word) net:tcp addr:[de:ad:be:ef::ca:fe]:80 dbname:dbname params:map[]}"},
	{"/dbname", "&{user: passwd: net:tcp addr:127.0.0.1:3306 dbname:dbname params:map[]}"},
	{"/", "&{user: passwd: net:tcp addr:127.0.0.1:3306 dbname: params:map[]}"},
	{"user:p@/ssword@/", "&{user:user passwd:p@/ssword net:tcp addr:127.0.0.1:3306 dbname: params:map[]}"},
}

func TestDSNParser(t *testing.T) {
	var cfg *config
	var res string

	for i, tst := range testDSNs {
		cfg = parseDSN(tst.in)
		res = fmt.Sprintf("%+v", cfg)
		if res != tst.out {
			t.Errorf("%d. parseDSN(%q) => %q, want %q", i, tst.in, res, tst.out)
		}
	}
}

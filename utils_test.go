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
	"time"
)

func TestDSNParser(t *testing.T) {
	var testDSNs = []struct {
		in  string
		out string
		loc *time.Location
	}{
		{"username:password@protocol(address)/dbname?param=value", "&{user:username passwd:password net:protocol addr:address dbname:dbname params:map[param:value] loc:%p}", time.UTC},
		{"user@unix(/path/to/socket)/dbname?charset=utf8", "&{user:user passwd: net:unix addr:/path/to/socket dbname:dbname params:map[charset:utf8] loc:%p}", time.UTC},
		{"user:password@tcp(localhost:5555)/dbname?charset=utf8", "&{user:user passwd:password net:tcp addr:localhost:5555 dbname:dbname params:map[charset:utf8] loc:%p}", time.UTC},
		{"user:password@tcp(localhost:5555)/dbname?charset=utf8mb4,utf8", "&{user:user passwd:password net:tcp addr:localhost:5555 dbname:dbname params:map[charset:utf8mb4,utf8] loc:%p}", time.UTC},
		{"user:password@/dbname?loc=UTC", "&{user:user passwd:password net:tcp addr:127.0.0.1:3306 dbname:dbname params:map[loc:UTC] loc:%p}", time.UTC},
		{"user:p@ss(word)@tcp([de:ad:be:ef::ca:fe]:80)/dbname?loc=Local", "&{user:user passwd:p@ss(word) net:tcp addr:[de:ad:be:ef::ca:fe]:80 dbname:dbname params:map[loc:Local] loc:%p}", time.Local},
		{"/dbname", "&{user: passwd: net:tcp addr:127.0.0.1:3306 dbname:dbname params:map[] loc:%p}", time.UTC},
		{"/", "&{user: passwd: net:tcp addr:127.0.0.1:3306 dbname: params:map[] loc:%p}", time.UTC},
		{"user:p@/ssword@/", "&{user:user passwd:p@/ssword net:tcp addr:127.0.0.1:3306 dbname: params:map[] loc:%p}", time.UTC},
	}

	var cfg *config
	var err error
	var res string

	for i, tst := range testDSNs {
		cfg, err = parseDSN(tst.in)
		if err != nil {
			t.Error(err.Error())
		}

		res = fmt.Sprintf("%+v", cfg)
		if res != fmt.Sprintf(tst.out, tst.loc) {
			t.Errorf("%d. parseDSN(%q) => %q, want %q", i, tst.in, res, fmt.Sprintf(tst.out, tst.loc))
		}
	}
}

func TestScanNullTime(t *testing.T) {
	var scanTests = []struct {
		in    interface{}
		error bool
		valid bool
		time  time.Time
	}{
		{tDate, false, true, tDate},
		{sDate, false, true, tDate},
		{[]byte(sDate), false, true, tDate},
		{tDateTime, false, true, tDateTime},
		{sDateTime, false, true, tDateTime},
		{[]byte(sDateTime), false, true, tDateTime},
		{tDate0, false, true, tDate0},
		{sDate0, false, true, tDate0},
		{[]byte(sDate0), false, true, tDate0},
		{sDateTime0, false, true, tDate0},
		{[]byte(sDateTime0), false, true, tDate0},
		{"", true, false, tDate0},
		{"1234", true, false, tDate0},
		{0, true, false, tDate0},
	}

	var nt = NullTime{}
	var err error

	for _, tst := range scanTests {
		err = nt.Scan(tst.in)
		if (err != nil) != tst.error {
			t.Errorf("%v: expected error status %b, got %b", tst.in, tst.error, (err != nil))
		}
		if nt.Valid != tst.valid {
			t.Errorf("%v: expected valid status %b, got %b", tst.in, tst.valid, nt.Valid)
		}
		if nt.Time != tst.time {
			t.Errorf("%v: expected time %v, got %v", tst.in, tst.time, nt.Time)
		}
	}
}

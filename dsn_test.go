// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2016 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"crypto/tls"
	"fmt"
	"net/url"
	"reflect"
	"testing"
	"time"
)

var testDSNs = []struct {
	in  string
	out *Config
}{{
	"username:password@protocol(address)/dbname?param=value",
	&Config{User: "username", Passwd: "password", Net: "protocol", Addr: "address", DBName: "dbname", Params: map[string]string{"param": "value"}, Loc: time.UTC, MaxAllowedPacket: defaultMaxAllowedPacket, Logger: defaultLogger, AllowNativePasswords: true, CheckConnLiveness: true},
}, {
	"username:password@protocol(address)/dbname?param=value&columnsWithAlias=true",
	&Config{User: "username", Passwd: "password", Net: "protocol", Addr: "address", DBName: "dbname", Params: map[string]string{"param": "value"}, Loc: time.UTC, MaxAllowedPacket: defaultMaxAllowedPacket, Logger: defaultLogger, AllowNativePasswords: true, CheckConnLiveness: true, ColumnsWithAlias: true},
}, {
	"username:password@protocol(address)/dbname?param=value&columnsWithAlias=true&multiStatements=true",
	&Config{User: "username", Passwd: "password", Net: "protocol", Addr: "address", DBName: "dbname", Params: map[string]string{"param": "value"}, Loc: time.UTC, MaxAllowedPacket: defaultMaxAllowedPacket, Logger: defaultLogger, AllowNativePasswords: true, CheckConnLiveness: true, ColumnsWithAlias: true, MultiStatements: true},
}, {
	"user@unix(/path/to/socket)/dbname?charset=utf8",
	&Config{User: "user", Net: "unix", Addr: "/path/to/socket", DBName: "dbname", Params: map[string]string{"charset": "utf8"}, Loc: time.UTC, MaxAllowedPacket: defaultMaxAllowedPacket, Logger: defaultLogger, AllowNativePasswords: true, CheckConnLiveness: true},
}, {
	"user:password@tcp(localhost:5555)/dbname?charset=utf8&tls=true",
	&Config{User: "user", Passwd: "password", Net: "tcp", Addr: "localhost:5555", DBName: "dbname", Params: map[string]string{"charset": "utf8"}, Loc: time.UTC, MaxAllowedPacket: defaultMaxAllowedPacket, Logger: defaultLogger, AllowNativePasswords: true, CheckConnLiveness: true, TLSConfig: "true"},
}, {
	"user:password@tcp(localhost:5555)/dbname?charset=utf8mb4,utf8&tls=skip-verify",
	&Config{User: "user", Passwd: "password", Net: "tcp", Addr: "localhost:5555", DBName: "dbname", Params: map[string]string{"charset": "utf8mb4,utf8"}, Loc: time.UTC, MaxAllowedPacket: defaultMaxAllowedPacket, Logger: defaultLogger, AllowNativePasswords: true, CheckConnLiveness: true, TLSConfig: "skip-verify"},
}, {
	"user:password@/dbname?loc=UTC&timeout=30s&readTimeout=1s&writeTimeout=1s&allowAllFiles=1&clientFoundRows=true&allowOldPasswords=TRUE&collation=utf8mb4_unicode_ci&maxAllowedPacket=16777216&tls=false&allowCleartextPasswords=true&parseTime=true&rejectReadOnly=true",
	&Config{User: "user", Passwd: "password", Net: "tcp", Addr: "127.0.0.1:3306", DBName: "dbname", Collation: "utf8mb4_unicode_ci", Loc: time.UTC, TLSConfig: "false", AllowCleartextPasswords: true, AllowNativePasswords: true, Timeout: 30 * time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second, Logger: defaultLogger, AllowAllFiles: true, AllowOldPasswords: true, CheckConnLiveness: true, ClientFoundRows: true, MaxAllowedPacket: 16777216, ParseTime: true, RejectReadOnly: true},
}, {
	"user:password@/dbname?allowNativePasswords=false&checkConnLiveness=false&maxAllowedPacket=0&allowFallbackToPlaintext=true",
	&Config{User: "user", Passwd: "password", Net: "tcp", Addr: "127.0.0.1:3306", DBName: "dbname", Loc: time.UTC, MaxAllowedPacket: 0, Logger: defaultLogger, AllowFallbackToPlaintext: true, AllowNativePasswords: false, CheckConnLiveness: false},
}, {
	"user:p@ss(word)@tcp([de:ad:be:ef::ca:fe]:80)/dbname?loc=Local",
	&Config{User: "user", Passwd: "p@ss(word)", Net: "tcp", Addr: "[de:ad:be:ef::ca:fe]:80", DBName: "dbname", Loc: time.Local, MaxAllowedPacket: defaultMaxAllowedPacket, Logger: defaultLogger, AllowNativePasswords: true, CheckConnLiveness: true},
}, {
	"/dbname",
	&Config{Net: "tcp", Addr: "127.0.0.1:3306", DBName: "dbname", Loc: time.UTC, MaxAllowedPacket: defaultMaxAllowedPacket, Logger: defaultLogger, AllowNativePasswords: true, CheckConnLiveness: true},
}, {
	"/dbname%2Fwithslash",
	&Config{Net: "tcp", Addr: "127.0.0.1:3306", DBName: "dbname/withslash", Loc: time.UTC, MaxAllowedPacket: defaultMaxAllowedPacket, Logger: defaultLogger, AllowNativePasswords: true, CheckConnLiveness: true},
}, {
	"@/",
	&Config{Net: "tcp", Addr: "127.0.0.1:3306", Loc: time.UTC, MaxAllowedPacket: defaultMaxAllowedPacket, Logger: defaultLogger, AllowNativePasswords: true, CheckConnLiveness: true},
}, {
	"/",
	&Config{Net: "tcp", Addr: "127.0.0.1:3306", Loc: time.UTC, MaxAllowedPacket: defaultMaxAllowedPacket, Logger: defaultLogger, AllowNativePasswords: true, CheckConnLiveness: true},
}, {
	"",
	&Config{Net: "tcp", Addr: "127.0.0.1:3306", Loc: time.UTC, MaxAllowedPacket: defaultMaxAllowedPacket, Logger: defaultLogger, AllowNativePasswords: true, CheckConnLiveness: true},
}, {
	"user:p@/ssword@/",
	&Config{User: "user", Passwd: "p@/ssword", Net: "tcp", Addr: "127.0.0.1:3306", Loc: time.UTC, MaxAllowedPacket: defaultMaxAllowedPacket, Logger: defaultLogger, AllowNativePasswords: true, CheckConnLiveness: true},
}, {
	"unix/?arg=%2Fsome%2Fpath.ext",
	&Config{Net: "unix", Addr: "/tmp/mysql.sock", Params: map[string]string{"arg": "/some/path.ext"}, Loc: time.UTC, MaxAllowedPacket: defaultMaxAllowedPacket, Logger: defaultLogger, AllowNativePasswords: true, CheckConnLiveness: true},
}, {
	"tcp(127.0.0.1)/dbname",
	&Config{Net: "tcp", Addr: "127.0.0.1:3306", DBName: "dbname", Loc: time.UTC, MaxAllowedPacket: defaultMaxAllowedPacket, Logger: defaultLogger, AllowNativePasswords: true, CheckConnLiveness: true},
}, {
	"tcp(de:ad:be:ef::ca:fe)/dbname",
	&Config{Net: "tcp", Addr: "[de:ad:be:ef::ca:fe]:3306", DBName: "dbname", Loc: time.UTC, MaxAllowedPacket: defaultMaxAllowedPacket, Logger: defaultLogger, AllowNativePasswords: true, CheckConnLiveness: true},
},
}

func TestDSNParser(t *testing.T) {
	for i, tst := range testDSNs {
		t.Run(tst.in, func(t *testing.T) {
			cfg, err := ParseDSN(tst.in)
			if err != nil {
				t.Error(err.Error())
				return
			}

			// pointer not static
			cfg.TLS = nil

			if !reflect.DeepEqual(cfg, tst.out) {
				t.Errorf("%d. ParseDSN(%q) mismatch:\ngot  %+v\nwant %+v", i, tst.in, cfg, tst.out)
			}
		})
	}
}

func TestDSNParserInvalid(t *testing.T) {
	var invalidDSNs = []string{
		"@net(addr/",                            // no closing brace
		"@tcp(/",                                // no closing brace
		"tcp(/",                                 // no closing brace
		"(/",                                    // no closing brace
		"net(addr)//",                           // unescaped
		"User:pass@tcp(1.2.3.4:3306)",           // no trailing slash
		"net()/",                                // unknown default addr
		"user:pass@tcp(127.0.0.1:3306)/db/name", // invalid dbname
		"user:password@/dbname?allowFallbackToPlaintext=PREFERRED", // wrong bool flag
		//"/dbname?arg=/some/unescaped/path",
	}

	for i, tst := range invalidDSNs {
		if _, err := ParseDSN(tst); err == nil {
			t.Errorf("invalid DSN #%d. (%s) didn't error!", i, tst)
		}
	}
}

func TestDSNReformat(t *testing.T) {
	for i, tst := range testDSNs {
		t.Run(tst.in, func(t *testing.T) {
			dsn1 := tst.in
			cfg1, err := ParseDSN(dsn1)
			if err != nil {
				t.Error(err.Error())
				return
			}
			cfg1.TLS = nil // pointer not static
			res1 := fmt.Sprintf("%+v", cfg1)

			dsn2 := cfg1.FormatDSN()
			if dsn2 != dsn1 {
				// Just log
				t.Logf("%d. %q reformatted as %q", i, dsn1, dsn2)
			}

			cfg2, err := ParseDSN(dsn2)
			if err != nil {
				t.Error(err.Error())
				return
			}
			cfg2.TLS = nil // pointer not static
			res2 := fmt.Sprintf("%+v", cfg2)

			if res1 != res2 {
				t.Errorf("%d. %q does not match %q", i, res2, res1)
			}

			dsn3 := cfg2.FormatDSN()
			if dsn3 != dsn2 {
				t.Errorf("%d. %q does not match %q", i, dsn2, dsn3)
			}
		})
	}
}

func TestDSNServerPubKey(t *testing.T) {
	baseDSN := "User:password@tcp(localhost:5555)/dbname?serverPubKey="

	RegisterServerPubKey("testKey", testPubKeyRSA)
	defer DeregisterServerPubKey("testKey")

	tst := baseDSN + "testKey"
	cfg, err := ParseDSN(tst)
	if err != nil {
		t.Error(err.Error())
	}

	if cfg.ServerPubKey != "testKey" {
		t.Errorf("unexpected cfg.ServerPubKey value: %v", cfg.ServerPubKey)
	}
	if cfg.pubKey != testPubKeyRSA {
		t.Error("pub key pointer doesn't match")
	}

	// Key is missing
	tst = baseDSN + "invalid_name"
	cfg, err = ParseDSN(tst)
	if err == nil {
		t.Errorf("invalid name in DSN (%s) but did not error. Got config: %#v", tst, cfg)
	}
}

func TestDSNServerPubKeyQueryEscape(t *testing.T) {
	const name = "&%!:"
	dsn := "User:password@tcp(localhost:5555)/dbname?serverPubKey=" + url.QueryEscape(name)

	RegisterServerPubKey(name, testPubKeyRSA)
	defer DeregisterServerPubKey(name)

	cfg, err := ParseDSN(dsn)
	if err != nil {
		t.Error(err.Error())
	}

	if cfg.pubKey != testPubKeyRSA {
		t.Error("pub key pointer doesn't match")
	}
}

func TestDSNWithCustomTLS(t *testing.T) {
	baseDSN := "User:password@tcp(localhost:5555)/dbname?tls="
	tlsCfg := tls.Config{}

	RegisterTLSConfig("utils_test", &tlsCfg)
	defer DeregisterTLSConfig("utils_test")

	// Custom TLS is missing
	tst := baseDSN + "invalid_tls"
	cfg, err := ParseDSN(tst)
	if err == nil {
		t.Errorf("invalid custom TLS in DSN (%s) but did not error. Got config: %#v", tst, cfg)
	}

	tst = baseDSN + "utils_test"

	// Custom TLS with a server name
	name := "foohost"
	tlsCfg.ServerName = name
	cfg, err = ParseDSN(tst)

	if err != nil {
		t.Error(err.Error())
	} else if cfg.TLS.ServerName != name {
		t.Errorf("did not get the correct TLS ServerName (%s) parsing DSN (%s).", name, tst)
	}

	// Custom TLS without a server name
	name = "localhost"
	tlsCfg.ServerName = ""
	cfg, err = ParseDSN(tst)

	if err != nil {
		t.Error(err.Error())
	} else if cfg.TLS.ServerName != name {
		t.Errorf("did not get the correct ServerName (%s) parsing DSN (%s).", name, tst)
	} else if tlsCfg.ServerName != "" {
		t.Errorf("tlsCfg was mutated ServerName (%s) should be empty parsing DSN (%s).", name, tst)
	}
}

func TestDSNTLSConfig(t *testing.T) {
	expectedServerName := "example.com"
	dsn := "tcp(example.com:1234)/?tls=true"

	cfg, err := ParseDSN(dsn)
	if err != nil {
		t.Error(err.Error())
	}
	if cfg.TLS == nil {
		t.Error("cfg.tls should not be nil")
	}
	if cfg.TLS.ServerName != expectedServerName {
		t.Errorf("cfg.tls.ServerName should be %q, got %q (host with port)", expectedServerName, cfg.TLS.ServerName)
	}

	dsn = "tcp(example.com)/?tls=true"
	cfg, err = ParseDSN(dsn)
	if err != nil {
		t.Error(err.Error())
	}
	if cfg.TLS == nil {
		t.Error("cfg.tls should not be nil")
	}
	if cfg.TLS.ServerName != expectedServerName {
		t.Errorf("cfg.tls.ServerName should be %q, got %q (host without port)", expectedServerName, cfg.TLS.ServerName)
	}
}

func TestDSNWithCustomTLSQueryEscape(t *testing.T) {
	const configKey = "&%!:"
	dsn := "User:password@tcp(localhost:5555)/dbname?tls=" + url.QueryEscape(configKey)
	name := "foohost"
	tlsCfg := tls.Config{ServerName: name}

	RegisterTLSConfig(configKey, &tlsCfg)
	defer DeregisterTLSConfig(configKey)

	cfg, err := ParseDSN(dsn)

	if err != nil {
		t.Error(err.Error())
	} else if cfg.TLS.ServerName != name {
		t.Errorf("did not get the correct TLS ServerName (%s) parsing DSN (%s).", name, dsn)
	}
}

func TestDSNUnsafeCollation(t *testing.T) {
	_, err := ParseDSN("/dbname?collation=gbk_chinese_ci&interpolateParams=true")
	if err != errInvalidDSNUnsafeCollation {
		t.Errorf("expected %v, got %v", errInvalidDSNUnsafeCollation, err)
	}

	_, err = ParseDSN("/dbname?collation=gbk_chinese_ci&interpolateParams=false")
	if err != nil {
		t.Errorf("expected %v, got %v", nil, err)
	}

	_, err = ParseDSN("/dbname?collation=gbk_chinese_ci")
	if err != nil {
		t.Errorf("expected %v, got %v", nil, err)
	}

	_, err = ParseDSN("/dbname?collation=ascii_bin&interpolateParams=true")
	if err != nil {
		t.Errorf("expected %v, got %v", nil, err)
	}

	_, err = ParseDSN("/dbname?collation=latin1_german1_ci&interpolateParams=true")
	if err != nil {
		t.Errorf("expected %v, got %v", nil, err)
	}

	_, err = ParseDSN("/dbname?collation=utf8_general_ci&interpolateParams=true")
	if err != nil {
		t.Errorf("expected %v, got %v", nil, err)
	}

	_, err = ParseDSN("/dbname?collation=utf8mb4_general_ci&interpolateParams=true")
	if err != nil {
		t.Errorf("expected %v, got %v", nil, err)
	}
}

func TestParamsAreSorted(t *testing.T) {
	expected := "/dbname?interpolateParams=true&foobar=baz&quux=loo"
	cfg := NewConfig()
	cfg.DBName = "dbname"
	cfg.InterpolateParams = true
	cfg.Params = map[string]string{
		"quux":   "loo",
		"foobar": "baz",
	}
	actual := cfg.FormatDSN()
	if actual != expected {
		t.Errorf("generic Config.Params were not sorted: want %#v, got %#v", expected, actual)
	}
}

func TestCloneConfig(t *testing.T) {
	RegisterServerPubKey("testKey", testPubKeyRSA)
	defer DeregisterServerPubKey("testKey")

	expectedServerName := "example.com"
	dsn := "tcp(example.com:1234)/?tls=true&foobar=baz&serverPubKey=testKey"
	cfg, err := ParseDSN(dsn)
	if err != nil {
		t.Fatal(err.Error())
	}

	cfg2 := cfg.Clone()
	if cfg == cfg2 {
		t.Errorf("Config.Clone did not create a separate config struct")
	}

	if cfg2.TLS.ServerName != expectedServerName {
		t.Errorf("cfg.tls.ServerName should be %q, got %q (host with port)", expectedServerName, cfg.TLS.ServerName)
	}

	cfg2.TLS.ServerName = "example2.com"
	if cfg.TLS.ServerName == cfg2.TLS.ServerName {
		t.Errorf("changed cfg.tls.Server name should not propagate to original Config")
	}

	if _, ok := cfg2.Params["foobar"]; !ok {
		t.Errorf("cloned Config is missing custom params")
	}

	delete(cfg2.Params, "foobar")

	if _, ok := cfg.Params["foobar"]; !ok {
		t.Errorf("custom params in cloned Config should not propagate to original Config")
	}

	if !reflect.DeepEqual(cfg.pubKey, cfg2.pubKey) {
		t.Errorf("public key in Config should be identical")
	}
}

func TestNormalizeTLSConfig(t *testing.T) {
	tt := []struct {
		tlsConfig string
		want      *tls.Config
	}{
		{"", nil},
		{"false", nil},
		{"true", &tls.Config{ServerName: "myserver"}},
		{"skip-verify", &tls.Config{InsecureSkipVerify: true}},
		{"preferred", &tls.Config{InsecureSkipVerify: true}},
		{"test_tls_config", &tls.Config{ServerName: "myServerName"}},
	}

	RegisterTLSConfig("test_tls_config", &tls.Config{ServerName: "myServerName"})
	defer func() { DeregisterTLSConfig("test_tls_config") }()

	for _, tc := range tt {
		t.Run(tc.tlsConfig, func(t *testing.T) {
			cfg := &Config{
				Addr:      "myserver:3306",
				TLSConfig: tc.tlsConfig,
			}

			cfg.normalize()

			if cfg.TLS == nil {
				if tc.want != nil {
					t.Fatal("wanted a tls config but got nil instead")
				}
				return
			}

			if cfg.TLS.ServerName != tc.want.ServerName {
				t.Errorf("tls.ServerName doesn't match (want: '%s', got: '%s')",
					tc.want.ServerName, cfg.TLS.ServerName)
			}
			if cfg.TLS.InsecureSkipVerify != tc.want.InsecureSkipVerify {
				t.Errorf("tls.InsecureSkipVerify doesn't match (want: %T, got :%T)",
					tc.want.InsecureSkipVerify, cfg.TLS.InsecureSkipVerify)
			}
		})
	}
}

func BenchmarkParseDSN(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for _, tst := range testDSNs {
			if _, err := ParseDSN(tst.in); err != nil {
				b.Error(err.Error())
			}
		}
	}
}

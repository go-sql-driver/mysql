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

// newTestConfig builds the expected ParseDSN result. Keep the defaults explicit
// instead of calling NewConfig so these tests can detect default regressions.
func newTestConfig(update func(*Config)) *Config {
	cfg := &Config{
		Net:                  "tcp",
		Addr:                 "127.0.0.1:3306",
		Loc:                  time.UTC,
		MaxAllowedPacket:     defaultMaxAllowedPacket,
		Logger:               defaultLogger,
		AllowNativePasswords: true,
		CheckConnLiveness:    true,
		tinyInt1IsBool:       true,
	}
	if update != nil {
		update(cfg)
	}
	return cfg
}

var testDSNs = []struct {
	in  string
	out *Config
}{
	{
		in: "username:password@protocol(address)/dbname?param=value",
		out: newTestConfig(func(cfg *Config) {
			cfg.User = "username"
			cfg.Passwd = "password"
			cfg.Net = "protocol"
			cfg.Addr = "address"
			cfg.DBName = "dbname"
			cfg.Params = map[string]string{"param": "value"}
			cfg.paramOrder = []string{"param"}
		}),
	},
	{
		in: "username:password@protocol(address)/dbname?param=value&columnsWithAlias=true",
		out: newTestConfig(func(cfg *Config) {
			cfg.User = "username"
			cfg.Passwd = "password"
			cfg.Net = "protocol"
			cfg.Addr = "address"
			cfg.DBName = "dbname"
			cfg.Params = map[string]string{"param": "value"}
			cfg.ColumnsWithAlias = true
			cfg.paramOrder = []string{"param"}
		}),
	},
	{
		in: "username:password@protocol(address)/dbname?param=value&columnsWithAlias=true&multiStatements=true",
		out: newTestConfig(func(cfg *Config) {
			cfg.User = "username"
			cfg.Passwd = "password"
			cfg.Net = "protocol"
			cfg.Addr = "address"
			cfg.DBName = "dbname"
			cfg.Params = map[string]string{"param": "value"}
			cfg.ColumnsWithAlias = true
			cfg.MultiStatements = true
			cfg.paramOrder = []string{"param"}
		}),
	},
	{
		in: "user@unix(/path/to/socket)/dbname?charset=utf8",
		out: newTestConfig(func(cfg *Config) {
			cfg.User = "user"
			cfg.Net = "unix"
			cfg.Addr = "/path/to/socket"
			cfg.DBName = "dbname"
			cfg.charsets = []string{"utf8"}
		}),
	},
	{
		in: "user:password@tcp(localhost:5555)/dbname?charset=utf8&tls=true",
		out: newTestConfig(func(cfg *Config) {
			cfg.User = "user"
			cfg.Passwd = "password"
			cfg.Addr = "localhost:5555"
			cfg.DBName = "dbname"
			cfg.TLSConfig = "true"
			cfg.charsets = []string{"utf8"}
		}),
	},
	{
		in: "user:password@tcp(localhost:5555)/dbname?charset=utf8mb4,utf8&tls=skip-verify",
		out: newTestConfig(func(cfg *Config) {
			cfg.User = "user"
			cfg.Passwd = "password"
			cfg.Addr = "localhost:5555"
			cfg.DBName = "dbname"
			cfg.TLSConfig = "skip-verify"
			cfg.charsets = []string{"utf8mb4", "utf8"}
		}),
	},
	{
		in: "user:password@/dbname?loc=UTC&timeout=30s&readTimeout=1s&writeTimeout=1s&allowAllFiles=1&clientFoundRows=true&allowOldPasswords=TRUE&collation=utf8mb4_unicode_ci&maxAllowedPacket=16777216&tls=false&allowCleartextPasswords=true&parseTime=true&rejectReadOnly=true",
		out: newTestConfig(func(cfg *Config) {
			cfg.User = "user"
			cfg.Passwd = "password"
			cfg.DBName = "dbname"
			cfg.Collation = "utf8mb4_unicode_ci"
			cfg.MaxAllowedPacket = 16777216
			cfg.TLSConfig = "false"
			cfg.Timeout = 30 * time.Second
			cfg.ReadTimeout = time.Second
			cfg.WriteTimeout = time.Second
			cfg.AllowAllFiles = true
			cfg.AllowCleartextPasswords = true
			cfg.AllowOldPasswords = true
			cfg.ClientFoundRows = true
			cfg.ParseTime = true
			cfg.RejectReadOnly = true
		}),
	},
	{
		in: "user:password@/dbname?allowNativePasswords=false&checkConnLiveness=false&maxAllowedPacket=0&allowFallbackToPlaintext=true",
		out: newTestConfig(func(cfg *Config) {
			cfg.User = "user"
			cfg.Passwd = "password"
			cfg.DBName = "dbname"
			cfg.MaxAllowedPacket = 0
			cfg.AllowFallbackToPlaintext = true
			cfg.AllowNativePasswords = false
			cfg.CheckConnLiveness = false
		}),
	},
	{
		in: "user:p@ss(word)@tcp([de:ad:be:ef::ca:fe]:80)/dbname?loc=Local",
		out: newTestConfig(func(cfg *Config) {
			cfg.User = "user"
			cfg.Passwd = "p@ss(word)"
			cfg.Addr = "[de:ad:be:ef::ca:fe]:80"
			cfg.DBName = "dbname"
			cfg.Loc = time.Local
		}),
	},
	{
		in: "/dbname",
		out: newTestConfig(func(cfg *Config) {
			cfg.DBName = "dbname"
		}),
	},
	{
		in: "/dbname%2Fwithslash",
		out: newTestConfig(func(cfg *Config) {
			cfg.DBName = "dbname/withslash"
		}),
	},
	{
		in:  "@/",
		out: newTestConfig(nil),
	},
	{
		in:  "/",
		out: newTestConfig(nil),
	},
	{
		in:  "",
		out: newTestConfig(nil),
	},
	{
		in: "user:p@/ssword@/",
		out: newTestConfig(func(cfg *Config) {
			cfg.User = "user"
			cfg.Passwd = "p@/ssword"
		}),
	},
	{
		in: "unix/?arg=%2Fsome%2Fpath.ext",
		out: newTestConfig(func(cfg *Config) {
			cfg.Net = "unix"
			cfg.Addr = "/tmp/mysql.sock"
			cfg.Params = map[string]string{"arg": "/some/path.ext"}
			cfg.paramOrder = []string{"arg"}
		}),
	},
	{
		in: "tcp(127.0.0.1)/dbname",
		out: newTestConfig(func(cfg *Config) {
			cfg.DBName = "dbname"
		}),
	},
	{
		in: "tcp(de:ad:be:ef::ca:fe)/dbname",
		out: newTestConfig(func(cfg *Config) {
			cfg.Addr = "[de:ad:be:ef::ca:fe]:3306"
			cfg.DBName = "dbname"
		}),
	},
	{
		in: "user:password@/dbname?loc=UTC&timeout=30s&parseTime=true&timeTruncate=1h",
		out: newTestConfig(func(cfg *Config) {
			cfg.User = "user"
			cfg.Passwd = "password"
			cfg.DBName = "dbname"
			cfg.Timeout = 30 * time.Second
			cfg.ParseTime = true
			cfg.timeTruncate = time.Hour
		}),
	},
	{
		in: "foo:bar@tcp(192.168.1.50:3307)/baz?timeout=10s&connectionAttributes=program_name:MySQLGoDriver%2FTest,program_version:1.2.3",
		out: newTestConfig(func(cfg *Config) {
			cfg.User = "foo"
			cfg.Passwd = "bar"
			cfg.Addr = "192.168.1.50:3307"
			cfg.DBName = "baz"
			cfg.ConnectionAttributes = "program_name:MySQLGoDriver/Test,program_version:1.2.3"
			cfg.Timeout = 10 * time.Second
		}),
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
		"user:password@/dbname?allowFallbackToPlaintext=PREFERRED",          // wrong bool flag
		"user:password@/dbname?connectionAttributes=attr1:/unescaped/value", // unescaped
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

func TestFormatDSN_NetWithoutAddr(t *testing.T) {
	// An explicit non-default Net should still appear in the formatted
	// DSN when Addr is empty, so the Config round-trips.
	cases := []struct {
		name string
		net  string
		want string
	}{
		{"unix without addr", "unix", "unix/"},
		// tcp is the default; dropping it is a no-op on parse.
		{"tcp without addr", "tcp", "/"},
		{"empty net empty addr", "", "/"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := NewConfig()
			cfg.Net = tc.net
			cfg.Addr = ""
			if got := cfg.FormatDSN(); got != tc.want {
				t.Errorf("FormatDSN() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestFormatDSN_AddrWithoutNet(t *testing.T) {
	// Direct check of the bug from #1616: an Addr-only Config should
	// format with the default tcp protocol so it round-trips.
	cfg := NewConfig()
	cfg.Addr = "myhost:3306"
	got := cfg.FormatDSN()
	want := "tcp(myhost:3306)/"
	if got != want {
		t.Errorf("FormatDSN() = %q, want %q", got, want)
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

func TestFormatDSNPreservesParamOrder(t *testing.T) {
	dsn := "/dbname?second=2&first=1"
	cfg, err := ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	want := "tcp(127.0.0.1:3306)" + dsn
	if got := cfg.FormatDSN(); got != want {
		t.Fatalf("FormatDSN() = %q, want %q", got, want)
	}
}

func TestAddParam(t *testing.T) {
	cfg := NewConfig()
	if err := cfg.Apply(
		AddParam("second", "2"),
		AddParam("first", "1"),
		AddParam("second", "new"),
	); err != nil {
		t.Fatal(err)
	}

	if got, want := cfg.FormatDSN(), "/?first=1&second=new"; got != want {
		t.Fatalf("FormatDSN() = %q, want %q", got, want)
	}
	if got, want := cfg.setParamsCommand(), "SET first = 1, second = new"; got != want {
		t.Fatalf("setParamsCommand() = %q, want %q", got, want)
	}
}

func TestAddParamAfterDirectParams(t *testing.T) {
	cfg := NewConfig()
	cfg.Params = map[string]string{"second": "2", "first": "1"}
	if err := cfg.Apply(AddParam("third", "3")); err != nil {
		t.Fatal(err)
	}

	if got, want := cfg.FormatDSN(), "/?first=1&second=2&third=3"; got != want {
		t.Fatalf("FormatDSN() = %q, want %q", got, want)
	}
}

func TestDSNParamOrder(t *testing.T) {
	dsn := "/?aurora_read_replica_read_committed=1&transaction_isolation=%27READ-COMMITTED%27"
	cfg, err := ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}

	want := "SET aurora_read_replica_read_committed = 1, transaction_isolation = 'READ-COMMITTED'"
	if got := cfg.setParamsCommand(); got != want {
		t.Fatalf("setParamsCommand() = %q, want %q", got, want)
	}
}

func TestDuplicateDSNParamOrder(t *testing.T) {
	cfg, err := ParseDSN("/?first=old&second=2&first=new")
	if err != nil {
		t.Fatal(err)
	}

	want := "SET second = 2, first = new"
	if got := cfg.setParamsCommand(); got != want {
		t.Fatalf("setParamsCommand() = %q, want %q", got, want)
	}
	if got, want := cfg.FormatDSN(), "tcp(127.0.0.1:3306)/?second=2&first=new"; got != want {
		t.Fatalf("FormatDSN() = %q, want %q", got, want)
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

	cfg2.paramOrder[0] = "changed"
	if cfg.paramOrder[0] == cfg2.paramOrder[0] {
		t.Errorf("param order in cloned Config should not propagate to original Config")
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

	for b.Loop() {
		for _, tst := range testDSNs {
			if _, err := ParseDSN(tst.in); err != nil {
				b.Error(err.Error())
			}
		}
	}
}

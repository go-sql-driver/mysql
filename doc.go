// Copyright 2026 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

// Package mysql provides a MySQL driver for Go's database/sql package.
//
// # Usage
//
// Import the driver for its registration side effect, then use the database/sql
// API with "mysql" as the driver name and a DSN as the data source name:
//
//	import (
//		"database/sql"
//		"time"
//
//		_ "github.com/go-sql-driver/mysql"
//	)
//
//	db, err := sql.Open("mysql", "user:password@/dbname")
//	if err != nil {
//		panic(err)
//	}
//	defer db.Close()
//	db.SetConnMaxLifetime(time.Minute * 3)
//	db.SetMaxOpenConns(10)
//	db.SetMaxIdleConns(10)
//
// sql.Open creates a connection pool; use db.PingContext to verify that the
// server is reachable. The pool is managed by database/sql. SetConnMaxLifetime
// should close connections before the server, OS, or middleware closes them.
// Some middleware closes idle connections after five minutes, so a lifetime
// shorter than five minutes is recommended. SetMaxOpenConns limits connections
// according to the needs of the application and server. Setting SetMaxIdleConns
// to the same limit avoids frequent connection creation and closure. Use
// SetConnMaxIdleTime to close idle connections sooner.
//
// To access driver-specific functions, import the package without the blank
// identifier. Use [NewConfig] to initialize a [Config] with defaults, then
// [Config.FormatDSN] to build a DSN, or [NewConnector] with sql.OpenDB to open a
// pool directly from the configuration.
//
// # DSN (Data Source Name)
//
// A DSN has the following format; parts in square brackets are optional:
//
//	[username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
//
// For example:
//
//	user:password@tcp(localhost:3306)/dbname?parseTime=true
//	user@unix(/path/to/socket)/dbname
//	user:password@tcp([::1]:3306)/dbname?timeout=90s
//	user:password@/dbname
//
// The default protocol is TCP and the default address is 127.0.0.1:3306.
// TCP addresses use host[:port], with port 3306 when omitted. Enclose literal
// IPv6 addresses in square brackets; net.JoinHostPort can construct addresses.
// For Unix sockets, the address is the absolute socket path. Custom networks
// can be registered with [RegisterDialContext].
//
// The minimal DSN selecting a database is /dbname. Leaving the database name
// empty ("/" or an empty DSN) opens a connection without selecting a database.
// Passwords may contain any character and do not need escaping. Database names
// are escaped with url.PathEscape; dbname/withslash becomes /dbname%2Fwithslash.
// Parameter values must be escaped with url.QueryEscape; for example,
// loc=US%2FPacific. [Config.FormatDSN] handles escaping when building a DSN.
//
// # DSN Parameters
//
// Parameter names are case-sensitive. Boolean parameters accept true, TRUE,
// True, or 1, and false, FALSE, False, or 0. Duration values are decimal numbers
// with unit suffixes, such as 30s, 0.5m, or 1m30s.
//
// allowAllFiles (bool, default false) disables the LOAD DATA LOCAL INFILE file
// allowlist. This permits access to all local files and may be insecure. Prefer
// [RegisterLocalFile] or [RegisterReaderHandler].
//
// allowCleartextPasswords (bool, default false) permits the cleartext client
// authentication plugin, for example for PAM accounts. Protect passwords with
// TLS, IPsec, or a private network when interception is possible.
//
// allowFallbackToPlaintext (bool, default false) permits an unencrypted
// connection when the server does not support TLS, like --ssl-mode=PREFERRED.
//
// allowNativePasswords (bool, default true) permits MySQL native password
// authentication. Set it to false to disallow that method.
//
// allowOldPasswords (bool, default false) permits the insecure old MySQL
// password method. Avoid enabling it unless required.
//
// charset (string, default none) sets the client-server charset using SET NAMES.
// A comma-separated list tries subsequent charsets if setting one fails, for
// example charset=utf8mb4,utf8. See the Unicode support section below.
//
// checkConnLiveness (bool, default true) checks pooled connections for liveness
// on supported platforms. Failed connections are marked bad and queries are
// retried on another connection. Set false to disable the check.
//
// collation (string, default utf8mb4_general_ci) selects the connection collation.
// SHOW COLLATION lists the server's collations. Collations for ucs2, utf16,
// utf16le, and utf32 cannot be used. See the Unicode support section below for
// how charset and collation are applied.
//
// clientFoundRows (bool, default false) makes UPDATE return the number of
// matching rows instead of the number of changed rows.
//
// columnsWithAlias (bool, default false) makes sql.Rows.Columns return names
// prefixed by table aliases, such as u.id for SELECT u.id FROM users AS u.
//
// compress (bool, default false) enables zlib compression.
//
// interpolateParams (bool, default false) interpolates placeholders in
// db.Query and db.Exec into a single query, reducing the round trips needed
// to prepare, execute, and close statements. BIG5, CP932, GB2312, GBK, and SJIS
// encodings are rejected with this option because they can introduce SQL
// injection vulnerabilities.
//
// loc (string, default UTC) selects the location of time.Time values when
// parseTime=true. Local selects the system location; other names are loaded
// with time.LoadLocation. Escape slashes, for example loc=US%2FPacific.
// This option does not change MySQL's time_zone system variable.
//
// timeTruncate (duration, default 0) truncates time.Time query arguments to the
// specified precision, for example 1us for DATETIME(6). Values read from the
// server are unaffected. On MariaDB, sending more fractional-second digits
// than an indexed column stores can prevent an index range scan; truncating
// arguments to the column's precision avoids this.
//
// tinyInt1IsBool (bool, default true) treats signed TINYINT(1) columns as
// booleans: zero is false and nonzero is true. Their database type name is
// BOOLEAN and scan type is bool or sql.NullBool for nullable columns. Unsigned
// and ZEROFILL columns are unaffected. Set false to retain numeric behavior.
//
// maxAllowedPacket (decimal number, default 67108864) sets the maximum packet
// size in bytes. Match the server's setting. A value of 0 fetches the server's
// max_allowed_packet value on every connection.
//
// multiStatements (bool, default false) permits multiple statements in one
// query. Use sql.Rows.NextResultSet to read subsequent results. Placeholders
// may occur only in the first statement; interpolateParams avoids this limit
// unless a prepared statement is used explicitly. To obtain affected rows and
// last insert IDs for each statement, use sql.Conn.Raw and [Result].
//
// parseTime (bool, default false) returns DATE and DATETIME values as time.Time
// instead of []byte or string. Zero dates, such as 0000-00-00 00:00:00, become
// the zero time.Time value. See the time.Time support section below.
//
// readTimeout and writeTimeout (duration, default 0) set the I/O read and write
// timeouts for individual connections. A value of 0 disables the timeout.
//
// rejectReadOnly (bool, default false) rejects read-only connections to avoid
// getting stuck on a read-only replica during failover, for example on AWS
// Aurora. It retries ERROR 1290, which also has other causes; enable it only
// when the application cannot produce that error except in read-only mode.
// Consider whether the application intentionally uses read-only transactions.
//
// serverPubKey (string, default none) names a public key registered with
// [RegisterServerPubKey]. Setting a known server key avoids retrieving it from
// the server whenever authentication requires it, which may be expensive and
// insecure.
//
// timeout (duration, default OS timeout) sets the connection dial timeout.
//
// tls (bool or string, default false) controls TLS. true enables encryption
// with certificate and server-name verification. skip-verify disables
// certificate verification. preferred also allows fallback to an unencrypted
// connection; neither skip-verify nor preferred provides reliable security.
// A name registered with [RegisterTLSConfig] selects a custom configuration.
// See the Verifying server section below.
//
// connectionAttributes (string, default none) supplies comma-separated
// key:value pairs sent to the server at connection time.
//
// # System Variables
//
// Other DSN parameters are interpreted as server system variables and set on
// connection. String values must be quoted with single quotes and then escaped
// with url.QueryEscape. For example:
//
//	autocommit=1
//	time_zone=%27Europe%2FParis%27
//	transaction_isolation=%27REPEATABLE-READ%27
//
// Variables are applied and retained by [Config.FormatDSN] in their DSN order.
// Use [Config.Apply] with [AddParam] to preserve order when adding variables
// programmatically.
//
// # Verifying Server
//
// Use tls=true to verify the certificate chain with system trust roots and
// verify the server name from the connection address. When dialing an IP for
// a DNS-named server, set the expected name explicitly in a [Config]:
//
//	cfg.TLS = &tls.Config{ServerName: "database.example"}
//
// For an exclusive private CA or the intended server's self-signed certificate,
// the following configuration verifies the chain without checking the name
// (VERIFY_CA). Obtain caPEM through a trusted channel:
//
//	roots := x509.NewCertPool()
//	if !roots.AppendCertsFromPEM(caPEM) {
//		return errors.New("no CA certificates found")
//	}
//	cfg.TLS = &tls.Config{
//		InsecureSkipVerify: true, // Replace default verification below.
//		VerifyConnection: func(state tls.ConnectionState) error {
//			if len(state.PeerCertificates) == 0 {
//				return errors.New("server did not provide a certificate")
//			}
//			intermediates := x509.NewCertPool()
//			for _, cert := range state.PeerCertificates[1:] {
//				intermediates.AddCert(cert)
//			}
//			_, err := state.PeerCertificates[0].Verify(x509.VerifyOptions{
//				Roots:         roots,
//				Intermediates: intermediates,
//				KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
//				// Omit DNSName for VERIFY_CA.
//			})
//			return err
//		},
//	}
//
// This authenticates the intended server only if the trusted signing key is
// kept private and has not issued certificates to unrelated servers or users.
// A CA shared with unrelated services does not provide that guarantee.
// InsecureSkipVerify with RootCAs alone does not verify certificates; retain
// the callback and propagate its errors. Pass cfg to [NewConnector] and
// sql.OpenDB, or register the TLS configuration with [RegisterTLSConfig] and
// select its name in the DSN. Avoid skip-verify, preferred, and plaintext
// fallback when server authentication is required.
//
// # Context and ColumnType Support
//
// The driver supports query timeouts and cancellation through context.Context.
// QueryContext, ExecContext, and similar database/sql methods close the
// connection if the context is canceled or times out before the driver receives
// the result.
//
// sql.ColumnType is supported except for ColumnType.Length. Unsigned integer
// database type names are prefixed with "UNSIGNED ".
//
// # Authentication Plugins
//
// Built-in authentication methods are mysql_native_password (the initial
// fallback), caching_sha2_password (the MySQL 8.0+ default), mysql_clear_password
// (requires allowCleartextPasswords), mysql_old_password (requires
// allowOldPasswords), sha256_password, and MariaDB's client_ed25519.
//
// Custom plugins implement [AuthPlugin] and are registered with
// [RegisterAuthPlugin]. Factories create independent instances for each
// authentication exchange and may run concurrently. Registering a built-in
// name replaces its factory for subsequent exchanges, including authentication
// switches; permissions for that name still apply.
//
// Plugins receive a context.Context and an [AuthContext] snapshot containing
// credentials, established transport information, and the configured RSA key.
// They do not receive or modify the connection's Config. Check any required
// transport in InitAuth before returning credentials. [AuthContext.TLS] reports
// a completed MySQL TLS handshake, not an external tunnel or TLS configuration.
// A continuation returning nil, nil waits without sending; a non-nil empty
// slice sends an empty packet. Success requires the server's OK packet.
//
// # LOAD DATA LOCAL INFILE Support
//
// Import mysql without the blank identifier to use [RegisterLocalFile] to
// allow specific file paths. The allowAllFiles parameter disables this check
// and may be insecure. [RegisterReaderHandler] makes an io.Reader available
// using the path Reader::<name>; its handler can also return an io.ReadCloser.
// Give each handler a distinct name and call [DeregisterReaderHandler] when
// it is no longer needed.
//
// # time.Time Support
//
// By default, DATE and DATETIME values are returned as []byte and can be scanned
// into []byte, string, or sql.RawBytes. Set parseTime=true to return time.Time
// values and use loc to choose their location. This changes the supported scan
// destinations and prevents scanning those values into sql.RawBytes.
//
// # Unicode Support
//
// The default collation is utf8mb4_general_ci. When only charset is specified,
// SET NAMES <charset> uses the server's default collation. When both charset and
// collation are specified, SET NAMES <charset> COLLATE <collation> is sent.
// With only collation, the driver specifies it in the protocol handshake and
// avoids a SET NAMES round trip, but the server may silently ignore it and use
// its default charset and collation.
//
// For further examples, see https://github.com/go-sql-driver/mysql/wiki/Examples.
// The README is available at https://github.com/go-sql-driver/mysql#usage.
package mysql

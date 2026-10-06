package mysql

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func oidcTestTLS(t *testing.T) (*tls.Config, *tls.Config) {
	t.Helper()
	certificate := authTestCertificate(t)
	cert, err := x509.ParseCertificate(certificate.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(cert)
	return &tls.Config{Certificates: []tls.Certificate{certificate}}, &tls.Config{RootCAs: roots, ServerName: "localhost"}
}

type oidcTestCapture struct {
	net.Conn
	raw bytes.Buffer
}

func (c *oidcTestCapture) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.raw.Write(p[:n])
	return n, err
}

type oidcTestResult struct {
	auth, extra, raw []byte
	err              error
}

// Each accepted connection gets an independent TLS session and a bounded deadline.
func oidcTestServer(t *testing.T, serverTLS *tls.Config, plugin string, caps capabilityFlag, reply []byte, count int) (string, <-chan oidcTestResult) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	results := make(chan oidcTestResult, count)
	go func() {
		for i := 0; i < count; i++ {
			conn, err := listener.Accept()
			if err != nil {
				results <- oidcTestResult{err: err}
				return
			}
			go func() {
				capture := &oidcTestCapture{Conn: conn}
				var r oidcTestResult
				defer func() {
					conn.Close()
					r.raw = capture.raw.Bytes()
					results <- r
				}()
				conn.SetDeadline(time.Now().Add(5 * time.Second))
				if _, err := conn.Write(authTestHandshake(plugin, caps)); err != nil {
					r.err = err
					return
				}
				var wire net.Conn = capture
				seq := byte(1)
				first, err := readAuthTestPacket(wire, seq)
				if err != nil {
					return
				} // Client rejected the greeting.
				if len(first) == 32 && binary.LittleEndian.Uint32(first)&uint32(clientSSL) != 0 {
					secured := tls.Server(wire, serverTLS)
					if err := secured.Handshake(); err != nil {
						return
					}
					wire = secured
					seq++
					first, err = readAuthTestPacket(wire, seq)
					if err != nil {
						return
					} // Verification rejected TLS before authentication.
				}
				r.auth = first
				if _, err := wire.Write(makePacket(seq+1, reply)); err != nil {
					r.err = err
					return
				}
				r.extra, _ = readAuthTestPacket(wire, seq+2)
			}()
		}
	}()
	return listener.Addr().String(), results
}

const oidcTestCaps = clientMySQL | clientProtocol41 | clientSecureConn | clientPluginAuth | clientPluginAuthLenEncClientData | clientSSL

var oidcTestOK = []byte{0, 0, 0, 2, 0, 0, 0}

func oidcTestAuth(t *testing.T, packet []byte, token string) {
	t.Helper()
	if len(packet) < 33 {
		t.Fatal("missing handshake response")
	}
	userEnd := bytes.IndexByte(packet[32:], 0)
	if userEnd < 0 {
		t.Fatal("missing username terminator")
	}
	pos := 32 + userEnd + 1
	// Decode independently of the driver's length-encoding helpers.
	readLen := func() int {
		t.Helper()
		if pos >= len(packet) {
			t.Fatal("missing auth length")
		}
		n := int(packet[pos])
		pos++
		if n == 252 {
			if pos+2 > len(packet) {
				t.Fatal("short lenenc")
			}
			n = int(binary.LittleEndian.Uint16(packet[pos:]))
			pos += 2
		}
		return n
	}
	outer := readLen()
	end := pos + outer
	if outer < 1 || end > len(packet) || packet[pos] != 1 {
		t.Fatal("invalid OIDC capability or outer length")
	}
	pos++
	inner := readLen()
	if inner != len(token) || pos+inner != end || string(packet[pos:end]) != token {
		t.Fatal("incorrect length-encoded OIDC token")
	}
	if string(packet[end:]) != openIDConnectPlugin+"\x00" {
		t.Fatalf("unexpected client plugin: %q", packet[end:])
	}
}

// Close successful connections so the fake server can finish capturing traffic.
func oidcTestConnect(t *testing.T, cfg *Config) error {
	t.Helper()
	c, err := NewConnector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := c.Connect(t.Context())
	if conn != nil {
		conn.Close()
	}
	return err
}

func TestOpenIDConnectProtocol(t *testing.T) {
	serverTLS, verified := oidcTestTLS(t)
	for _, greeting := range []string{"mysql_native_password", "caching_sha2_password", openIDConnectPlugin, "unknown_plugin"} {
		for _, size := range []int{20, 250, 251, 1024} {
			t.Run(fmt.Sprintf("%s/%d", greeting, size), func(t *testing.T) {
				token := strings.Repeat("x", size)
				address, results := oidcTestServer(t, serverTLS, greeting, oidcTestCaps, oidcTestOK, 1)
				cfg := NewConfig()
				cfg.Addr = address
				cfg.TLS = verified.Clone()
				cfg.AllowNativePasswords = false
				cfg.Apply(OIDCToken(token))
				if err := oidcTestConnect(t, cfg); err != nil {
					t.Fatal(err)
				}
				r := <-results
				if r.err != nil {
					t.Fatal(r.err)
				}
				oidcTestAuth(t, r.auth, token)
				if bytes.Contains(r.raw, []byte(token)) {
					t.Fatal("token visible on raw transport")
				}
			})
		}
	}
}

func TestOpenIDConnectTLS(t *testing.T) {
	serverTLS, verified := oidcTestTLS(t)
	for _, tc := range []struct {
		name      string
		configure func(*Config)
		remove    capabilityFlag
		success   bool
	}{
		{name: "verified", success: true},
		{name: "wrong_host", configure: func(c *Config) { c.TLS.ServerName = "wrong.test" }},
		{name: "unknown_ca", configure: func(c *Config) { c.TLS.RootCAs = x509.NewCertPool() }},
		// Verification policy belongs to the application, including custom pinning.
		{name: "skip_verify", success: true, configure: func(c *Config) { c.TLS.InsecureSkipVerify = true }},
		{name: "custom_verification", success: true, configure: func(c *Config) {
			c.TLS.InsecureSkipVerify = true
			c.TLS.ServerName = "wrong.test"
			c.TLS.VerifyConnection = func(state tls.ConnectionState) error {
				if !bytes.Equal(state.PeerCertificates[0].Raw, serverTLS.Certificates[0].Certificate[0]) {
					return errors.New("pin mismatch")
				}
				return nil
			}
		}},
		{name: "callback_rejects", configure: func(c *Config) {
			c.TLS.VerifyConnection = func(tls.ConnectionState) error { return errors.New("rejected") }
		}},
		{name: "no_tls", remove: clientSSL},
		{name: "preferred_fallback", remove: clientSSL, configure: func(c *Config) { c.AllowFallbackToPlaintext = true }},
		{name: "missing_plugin_auth", remove: clientPluginAuth},
		{name: "missing_lenenc", remove: clientPluginAuthLenEncClientData},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := NewConfig()
			cfg.TLS = verified.Clone()
			if tc.configure != nil {
				tc.configure(cfg)
			}
			token := "synthetic-sensitive-oidc-bearer-token"
			var logs bytes.Buffer
			cfg.Logger = log.New(&logs, "", 0)
			cfg.AllowCleartextPasswords = true
			cfg.Apply(OIDCToken(token))
			address, results := oidcTestServer(t, serverTLS, "mysql_native_password", oidcTestCaps&^tc.remove, oidcTestOK, 1)
			cfg.Addr = address
			err := oidcTestConnect(t, cfg)
			if (err == nil) != tc.success {
				t.Fatalf("Connect error = %v, want success = %v", err, tc.success)
			}
			r := <-results
			if r.err != nil {
				t.Fatal(r.err)
			}
			if tc.success {
				oidcTestAuth(t, r.auth, token)
			} else if len(r.auth) != 0 {
				t.Fatal("auth packet sent before successful verification")
			}
			if bytes.Contains(r.raw, []byte(token)) || strings.Contains(logs.String(), token) || (err != nil && strings.Contains(err.Error(), token)) {
				t.Fatal("token leaked")
			}
		})
	}
}

func TestOpenIDConnectConfig(t *testing.T) {
	cfg := NewConfig()
	cfg.Passwd = "existing-password"
	before := cfg.FormatDSN()
	cfg.Apply(OIDCToken("test-token"))
	clone := cfg.Clone()
	clone.Apply(OIDCToken("rotated-token"))
	if cfg.openIDToken != "test-token" || clone.openIDToken != "rotated-token" {
		t.Fatal("clone changed original")
	}
	if cfg.FormatDSN() != before || clone.FormatDSN() != before {
		t.Fatal("OIDC affected DSN")
	}
	parsed, err := ParseDSN(cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	if parsed.openIDToken != "" {
		t.Fatal("OIDC unexpectedly survived DSN round trip")
	}
	for _, current := range []string{"", "test-token"} {
		candidate := NewConfig()
		if current != "" {
			if err := candidate.Apply(OIDCToken(current)); err != nil {
				t.Fatal(err)
			}
		}
		if err := candidate.Apply(OIDCToken("")); !errors.Is(err, ErrOpenIDConnectToken) {
			t.Fatalf("empty token: got %v", err)
		}
		if candidate.openIDToken != current {
			t.Fatal("empty token changed configuration")
		}
	}

}

func TestOpenIDConnectBeforeConnectConcurrent(t *testing.T) {
	serverTLS, verified := oidcTestTLS(t)
	const count = 16
	address, results := oidcTestServer(t, serverTLS, "mysql_native_password", oidcTestCaps, oidcTestOK, count)
	cfg := NewConfig()
	cfg.Addr = address
	cfg.DialFunc = (&net.Dialer{}).DialContext
	var serial atomic.Int32
	cfg.Apply(BeforeConnect(func(_ context.Context, effective *Config) error {
		effective.TLS = verified.Clone()
		return effective.Apply(OIDCToken(fmt.Sprintf("rotated-%02d", serial.Add(1))))
	}))
	c, err := NewConnector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := c.Connect(context.Background())
			if err != nil {
				t.Error(err)
				return
			}
			conn.Close()
		}()
	}
	wg.Wait()
	seen := make(map[string]bool)
	for i := 0; i < count; i++ {
		r := <-results
		if r.err != nil {
			t.Fatal(r.err)
		}
		for j := 1; j <= count; j++ {
			token := fmt.Sprintf("rotated-%02d", j)
			if bytes.Contains(r.auth, []byte(token)) {
				oidcTestAuth(t, r.auth, token)
				if seen[token] {
					t.Fatal("token reused")
				}
				seen[token] = true
			}
		}
	}
	if len(seen) != count {
		t.Fatalf("got %d rotated tokens", len(seen))
	}
	if cfg.openIDToken != "" || c.(*connector).cfg.openIDToken != "" || cfg.TLS != nil || c.(*connector).cfg.TLS != nil {
		t.Fatal("BeforeConnect modified shared config")
	}
}

func TestOpenIDConnectRequiresDriverTLS(t *testing.T) {
	// A custom dialer returning TLS cannot establish the driver's verification
	// policy. Reject the configuration before dialing, including Unix sockets.
	for _, network := range []string{"tcp", "unix"} {
		t.Run(network, func(t *testing.T) {
			cfg := NewConfig()
			cfg.Net = network
			cfg.AllowCleartextPasswords = true
			cfg.DialFunc = func(context.Context, string, string) (net.Conn, error) {
				t.Error("dialer called without driver TLS configuration")
				return nil, errors.New("unexpected")
			}
			cfg.Apply(OIDCToken("test-token"))
			if err := oidcTestConnect(t, cfg); !errors.Is(err, ErrOpenIDConnectTLS) {
				t.Fatalf("got %v", err)
			}
		})
	}
}

func TestOpenIDConnectBeforeConnectEmpty(t *testing.T) {
	cfg := NewConfig()
	cfg.Apply(OIDCToken("stale-token"), BeforeConnect(func(_ context.Context, cfg *Config) error { return cfg.Apply(OIDCToken("")) }))
	cfg.DialFunc = func(context.Context, string, string) (net.Conn, error) {
		t.Error("dialed after empty refresh")
		return nil, errors.New("unexpected")
	}
	c, err := NewConnector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Connect(context.Background()); !errors.Is(err, ErrOpenIDConnectToken) {
		t.Fatalf("got %v", err)
	}
	if c.(*connector).cfg.openIDToken != "stale-token" {
		t.Fatal("shared token changed")
	}
}

func TestOpenIDConnectAuthResult(t *testing.T) {
	serverTLS, verified := oidcTestTLS(t)
	token := "synthetic-sensitive-oidc-token"
	type response struct {
		name  string
		reply []byte
		want  error
	}
	cases := []response{
		{"more_data", []byte{1, 3}, ErrMalformPkt},
		{"empty_more_data", []byte{1}, ErrMalformPkt},
		{"server_error", append([]byte{0xff, 0x15, 0x04, '#', '2', '8', '0', '0', '0'}, []byte("rejected "+token)...), &MySQLError{Number: 1045}},
	}
	for _, plugin := range []string{"mysql_native_password", "mysql_clear_password", openIDConnectPlugin, ""} {
		cases = append(cases, response{
			"switch/" + plugin, append([]byte{0xfe}, []byte(plugin+"\x00abcdefghijklmnopqrst\x00")...), ErrOpenIDConnectSwitch,
		})
	}
	cases = append(cases, response{"legacy_switch", []byte{0xfe}, ErrOpenIDConnectSwitch})
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			address, results := oidcTestServer(t, serverTLS, "mysql_native_password", oidcTestCaps, tc.reply, 1)
			cfg := NewConfig()
			cfg.Addr = address
			cfg.TLS = verified.Clone()
			cfg.Apply(OIDCToken(token))
			cfg.Passwd = "must-not-be-sent"
			cfg.AllowCleartextPasswords = true
			err := oidcTestConnect(t, cfg)
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
			if strings.Contains(err.Error(), token) {
				t.Fatal("server error leaked token")
			}
			r := <-results
			if r.err != nil {
				t.Fatal(r.err)
			}
			if len(r.extra) != 0 {
				t.Fatal("unexpected authentication response")
			}
		})
	}
}

package mysql

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func oidcTestTLS(t *testing.T) (*tls.Config, *tls.Config) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	root := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "OIDC test CA"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, root, root, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), DNSNames: []string{"oidc.test"}, NotBefore: root.NotBefore, NotAfter: root.NotAfter, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	leafDER, err := x509.CreateCertificate(rand.Reader, leaf, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	return &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{leafDER, der}, PrivateKey: key}}}, &tls.Config{RootCAs: roots, ServerName: "oidc.test"}
}

func oidcTestGreeting(plugin string, caps capabilityFlag) []byte {
	data := []byte("\x0a8.4.0\x00\x01\x00\x00\x00abcdefgh\x00")
	data = binary.LittleEndian.AppendUint16(data, uint16(caps))
	data = append(data, 45, 2, 0)
	data = binary.LittleEndian.AppendUint16(data, uint16(caps>>16))
	data = append(data, 21)
	data = append(data, make([]byte, 10)...)
	data = append(data, "ijklmnopqrst\x00"...)
	data = append(data, plugin...)
	return append(data, 0)
}

func oidcTestWrite(conn net.Conn, seq byte, data []byte) error {
	packet := append([]byte{byte(len(data)), byte(len(data) >> 8), byte(len(data) >> 16), seq}, data...)
	_, err := conn.Write(packet)
	return err
}

func oidcTestRead(conn net.Conn) ([]byte, byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		return nil, 0, err
	}
	n := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
	if n > 1<<20 {
		return nil, 0, fmt.Errorf("oversized test packet")
	}
	data := make([]byte, n)
	_, err := io.ReadFull(conn, data)
	return data, header[3], err
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
				defer func() { conn.Close(); r.raw = append([]byte(nil), capture.raw.Bytes()...); results <- r }()
				conn.SetDeadline(time.Now().Add(5 * time.Second))
				if err := oidcTestWrite(conn, 0, oidcTestGreeting(plugin, caps)); err != nil {
					r.err = err
					return
				}
				var wire net.Conn = capture
				first, seq, err := oidcTestRead(wire)
				if err != nil {
					return
				} // Client rejected the greeting.
				if len(first) == 32 && binary.LittleEndian.Uint32(first)&uint32(clientSSL) != 0 {
					if seq != 1 {
						r.err = fmt.Errorf("SSLRequest sequence = %d", seq)
						return
					}
					secured := tls.Server(wire, serverTLS)
					if err := secured.Handshake(); err != nil {
						return
					}
					wire = secured
					first, seq, err = oidcTestRead(wire)
					if err != nil {
						return
					} // Verification rejected TLS before authentication.
					if seq != 2 {
						r.err = fmt.Errorf("TLS auth sequence = %d", seq)
						return
					}
				}
				r.auth = first
				if err := oidcTestWrite(wire, seq+1, reply); err != nil {
					r.err = err
					return
				}
				r.extra, _, _ = oidcTestRead(wire)
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
	pos := 32 + bytes.IndexByte(packet[32:], 0) + 1
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
	if end > len(packet) || packet[pos] != 1 {
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
				c, err := NewConnector(cfg)
				if err != nil {
					t.Fatal(err)
				}
				conn, err := c.Connect(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				conn.Close()
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
	for _, name := range []string{"verified", "verify_ca", "pinning", "wrong_host", "unknown_ca", "skip_verify", "noop_callback", "callback_rejects", "verify_ca_wrong_roots", "verify_ca_expired", "no_tls", "preferred_fallback", "missing_plugin_auth", "missing_lenenc"} {
		t.Run(name, func(t *testing.T) {
			cfg := NewConfig()
			cfg.TLS = verified.Clone()
			caps := oidcTestCaps
			success := false
			switch name {
			case "verified":
				success = true
			case "verify_ca":
				success = true
				cfg.TLS.InsecureSkipVerify = true
				cfg.TLS.ServerName = "unrelated.test"
			case "wrong_host":
				cfg.TLS.ServerName = "unrelated.test"
			case "unknown_ca":
				cfg.TLS.RootCAs = x509.NewCertPool()
			case "skip_verify":
				success = true // Verification policy belongs to the application.
				cfg.TLS = &tls.Config{InsecureSkipVerify: true}
			case "noop_callback":
				success = true // The driver does not judge callback correctness.
				cfg.TLS = &tls.Config{InsecureSkipVerify: true, VerifyConnection: func(tls.ConnectionState) error { return nil }}
			case "callback_rejects":
				cfg.TLS.VerifyConnection = func(tls.ConnectionState) error { return errors.New("rejected") }
			case "verify_ca_wrong_roots":
				cfg.TLS.InsecureSkipVerify = true
				cfg.TLS.RootCAs = x509.NewCertPool()
			case "verify_ca_expired":
				cfg.TLS.InsecureSkipVerify = true
				cfg.TLS.Time = func() time.Time { return time.Now().Add(24 * time.Hour) }
			case "no_tls":
				caps &^= clientSSL
			case "preferred_fallback":
				caps &^= clientSSL
				cfg.TLS = &tls.Config{InsecureSkipVerify: true}
				cfg.AllowFallbackToPlaintext = true
			case "missing_plugin_auth":
				caps &^= clientPluginAuth
			case "missing_lenenc":
				caps &^= clientPluginAuthLenEncClientData
			}
			if strings.HasPrefix(name, "verify_ca") {
				roots := cfg.TLS.RootCAs
				cfg.TLS.VerifyConnection = func(state tls.ConnectionState) error {
					intermediates := x509.NewCertPool()
					for _, cert := range state.PeerCertificates[1:] {
						intermediates.AddCert(cert)
					}
					opts := x509.VerifyOptions{Roots: roots, Intermediates: intermediates}
					if cfg.TLS.Time != nil {
						opts.CurrentTime = cfg.TLS.Time()
					}
					_, err := state.PeerCertificates[0].Verify(opts)
					return err
				}
			}
			if name == "pinning" {
				success = true
				cfg.TLS = &tls.Config{InsecureSkipVerify: true, VerifyConnection: func(state tls.ConnectionState) error {
					if !bytes.Equal(state.PeerCertificates[0].Raw, serverTLS.Certificates[0].Certificate[0]) {
						return errors.New("pin mismatch")
					}
					return nil
				}}
			}
			token := "synthetic-sensitive-oidc-bearer-token"
			var logs bytes.Buffer
			cfg.Logger = log.New(&logs, "", 0)
			cfg.AllowCleartextPasswords = true
			cfg.Apply(OIDCToken(token))
			address, results := oidcTestServer(t, serverTLS, "mysql_native_password", caps, oidcTestOK, 1)
			cfg.Addr = address
			c, err := NewConnector(cfg)
			if err != nil {
				t.Fatal(err)
			}
			conn, err := c.Connect(context.Background())
			if success {
				if err != nil {
					t.Fatal(err)
				}
				conn.Close()
			} else if err == nil {
				conn.Close()
				t.Fatal("insecure connection accepted")
			}
			r := <-results
			if r.err != nil {
				t.Fatal(r.err)
			}
			if success {
				oidcTestAuth(t, r.auth, token)
			} else if len(r.auth) != 0 {
				t.Fatal("auth packet sent before successful verification")
			}
			if bytes.Contains(r.raw, []byte(token)) || strings.Contains(logs.String(), token) || (err != nil && strings.Contains(err.Error(), token)) {
				t.Fatal("token leaked")
			}
			if c.(*connector).cfg.TLS == nil {
				t.Fatal("shared TLS config mutated")
			}
		})
	}
}

func TestOpenIDConnectRejectSwitch(t *testing.T) {
	serverTLS, verified := oidcTestTLS(t)
	for _, plugin := range []string{"mysql_native_password", "mysql_clear_password", openIDConnectPlugin, "", "legacy"} {
		t.Run(plugin, func(t *testing.T) {
			reply := append([]byte{0xfe}, plugin...)
			reply = append(reply, 0)
			reply = append(reply, "abcdefghijklmnopqrst\x00"...)
			if plugin == "legacy" {
				reply = []byte{0xfe}
			}
			address, results := oidcTestServer(t, serverTLS, "caching_sha2_password", oidcTestCaps, reply, 1)
			cfg := NewConfig()
			cfg.Addr = address
			cfg.TLS = verified.Clone()
			cfg.Passwd = "must-not-be-sent"
			cfg.AllowCleartextPasswords = true
			cfg.Apply(OIDCToken("test-token"))
			c, err := NewConnector(cfg)
			if err != nil {
				t.Fatal(err)
			}
			conn, err := c.Connect(context.Background())
			if conn != nil {
				conn.Close()
			}
			if !errors.Is(err, ErrOpenIDConnectSwitch) {
				t.Fatalf("got %v", err)
			}
			r := <-results
			if r.err != nil {
				t.Fatal(r.err)
			}
			if len(r.extra) != 0 {
				t.Fatal("client responded to auth switch")
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
	cfg.DialFunc = func(context.Context, string, string) (net.Conn, error) {
		t.Error("invalid config dialed")
		return nil, errors.New("unexpected dial")
	}
	c, err := NewConnector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Connect(context.Background()); !errors.Is(err, ErrOpenIDConnectTLS) {
		t.Fatalf("got %v, want %v", err, ErrOpenIDConnectTLS)
	}
}

func TestOpenIDConnectBeforeConnectConcurrent(t *testing.T) {
	serverTLS, verified := oidcTestTLS(t)
	const count = 16
	address, results := oidcTestServer(t, serverTLS, "mysql_native_password", oidcTestCaps, oidcTestOK, count)
	cfg := NewConfig()
	cfg.Addr = address
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

func TestOpenIDConnectConcurrentFallback(t *testing.T) {
	serverTLS, verified := oidcTestTLS(t)
	const count = 12
	good, goodResults := oidcTestServer(t, serverTLS, "mysql_native_password", oidcTestCaps, oidcTestOK, count/2)
	bad, badResults := oidcTestServer(t, serverTLS, "mysql_native_password", oidcTestCaps&^clientSSL, oidcTestOK, count/2)
	cfg := NewConfig()
	cfg.TLS = verified.Clone()
	cfg.AllowFallbackToPlaintext = true
	cfg.Apply(OIDCToken("concurrent-test-token"))
	var serial atomic.Int32
	// A custom dialer carrying the normal MySQL TLS upgrade is supported.
	cfg.DialFunc = func(ctx context.Context, network, _ string) (net.Conn, error) {
		address := good
		if serial.Add(1)%2 == 0 {
			address = bad
		}
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}
	c, err := NewConnector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var successes atomic.Int32
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := c.Connect(context.Background())
			if err == nil {
				successes.Add(1)
				conn.Close()
			} else if !errors.Is(err, ErrNoTLS) {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if successes.Load() != count/2 {
		t.Fatalf("got %d successes", successes.Load())
	}
	for i := 0; i < count/2; i++ {
		r := <-goodResults
		if r.err != nil {
			t.Fatal(r.err)
		}
		oidcTestAuth(t, r.auth, "concurrent-test-token")
		r = <-badResults
		if r.err != nil {
			t.Fatal(r.err)
		}
		if len(r.auth) != 0 {
			t.Fatal("auth sent after TLS downgrade")
		}
	}
	if c.(*connector).cfg.TLS == nil {
		t.Fatal("shared TLS config was cleared")
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
			c, err := NewConnector(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = c.Connect(context.Background()); !errors.Is(err, ErrOpenIDConnectTLS) {
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
	for _, tc := range []struct {
		name  string
		reply []byte
		want  error
	}{
		{"more_data", []byte{1, 3}, ErrMalformPkt},
		{"empty_more_data", []byte{1}, ErrMalformPkt},
		{"server_error", append([]byte{0xff, 0x15, 0x04, '#', '2', '8', '0', '0', '0'}, []byte("rejected "+token)...), &MySQLError{Number: 1045}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			address, results := oidcTestServer(t, serverTLS, "mysql_native_password", oidcTestCaps, tc.reply, 1)
			cfg := NewConfig()
			cfg.Addr = address
			cfg.TLS = verified.Clone()
			cfg.Apply(OIDCToken(token))
			c, err := NewConnector(cfg)
			if err != nil {
				t.Fatal(err)
			}
			conn, err := c.Connect(context.Background())
			if conn != nil {
				conn.Close()
			}
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

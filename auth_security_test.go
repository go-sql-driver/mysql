package mysql

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"testing"
)

func TestConnectorRejectsUnsolicitedPublicKey(t *testing.T) {
	otherKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKIXPublicKey(&otherKey.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	keyData := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})
	for _, plugin := range []string{"caching_sha2_password", "sha256_password"} {
		t.Run(plugin, func(t *testing.T) {
			caps := clientMySQL | clientProtocol41 | clientSecureConn | clientPluginAuth | clientPluginAuthLenEncClientData
			mock := &mockConn{
				data:          authTestHandshake(plugin, caps),
				queuedReplies: [][]byte{makePacket(2, append([]byte{iAuthMoreData}, keyData...))},
				maxReads:      2,
			}
			cfg := NewConfig()
			cfg.User, cfg.Passwd = "user", "secret"
			cfg.pubKey = testPubKeyRSA
			cfg.DialFunc = func(context.Context, string, string) (net.Conn, error) { return mock, nil }
			c, err := NewConnector(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.Connect(t.Context()); !errors.Is(err, ErrMalformPkt) {
				t.Fatalf("Connect = %v, want ErrMalformPkt", err)
			}
			// Only the initial handshake may be sent, never a password encrypted
			// using the peer's unsolicited key (nor COM_QUIT after an auth error).
			if mock.writes != 1 {
				t.Fatalf("writes = %d, want only the handshake response", mock.writes)
			}
		})
	}
}

func TestAuthContinuationStates(t *testing.T) {
	for _, tc := range []struct {
		name    string
		plugin  string
		tls     bool
		unix    bool
		pinned  bool
		empty   bool
		prefix  [][]byte
		invalid [][]byte
	}{
		{name: "caching initial", plugin: "caching_sha2_password"},
		{name: "caching fast", plugin: "caching_sha2_password", prefix: [][]byte{{3}}},
		{name: "caching full TLS", plugin: "caching_sha2_password", tls: true, prefix: [][]byte{{4}}},
		{name: "caching full unix", plugin: "caching_sha2_password", unix: true, prefix: [][]byte{{4}}},
		{name: "caching full pinned", plugin: "caching_sha2_password", pinned: true, prefix: [][]byte{{4}}},
		{name: "caching key consumed", plugin: "caching_sha2_password", prefix: [][]byte{{4}, testPubKey}},
		{name: "caching awaiting key", plugin: "caching_sha2_password", prefix: [][]byte{{4}}, invalid: [][]byte{nil, {3}, {4}}},
		{name: "sha256 pinned", plugin: "sha256_password", pinned: true},
		{name: "sha256 TLS", plugin: "sha256_password", tls: true},
		{name: "sha256 empty password", plugin: "sha256_password", empty: true},
		{name: "sha256 key consumed", plugin: "sha256_password", prefix: [][]byte{testPubKey}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			invalid := tc.invalid
			if invalid == nil {
				invalid = [][]byte{testPubKey, nil}
				if len(tc.prefix) > 0 {
					invalid = append(invalid, []byte{3}, []byte{4})
				}
			}
			for i, packet := range invalid {
				t.Run(fmt.Sprint(i), func(t *testing.T) {
					_, mc := newRWMockConn(2)
					if !tc.empty {
						mc.cfg.Passwd = "secret"
					}
					if tc.pinned {
						mc.cfg.pubKey = testPubKeyRSA
					}
					if tc.unix {
						mc.cfg.Net = "unix"
					}
					seed := []byte("0123456789abcdefghij")
					auth := newAuthContext(mc.cfg, mc.cfg.Passwd, tc.tls)
					plugin, _, err := mc.initAuth(t.Context(), tc.plugin, seed, auth)
					if err != nil {
						t.Fatal(err)
					}
					for _, step := range tc.prefix {
						if _, err := plugin.ContinuationAuth(t.Context(), step, seed, auth); err != nil {
							t.Fatalf("valid prefix: %v", err)
						}
					}
					response, err := plugin.ContinuationAuth(t.Context(), packet, seed, auth)
					if !errors.Is(err, ErrMalformPkt) || response != nil {
						t.Fatalf("ContinuationAuth = (%x, %v), want (nil, ErrMalformPkt)", response, err)
					}
				})
			}
		})
	}
}

func TestAuthSwitchRejectsShortChallenge(t *testing.T) {
	for _, tc := range []struct {
		plugin string
		length int
	}{
		{"mysql_native_password", 0},
		{"mysql_native_password", 1},
		{"mysql_native_password", 19},
		{"mysql_old_password", 0},
		{"mysql_old_password", 1},
		{"mysql_old_password", 7},
	} {
		t.Run(fmt.Sprintf("%s/%d", tc.plugin, tc.length), func(t *testing.T) {
			conn, mc := newRWMockConn(2)
			mc.cfg.Passwd = "secret"
			mc.cfg.AllowOldPasswords = true
			payload := append([]byte{iEOF}, tc.plugin...)
			payload = append(payload, 0)
			payload = append(payload, bytes.Repeat([]byte{'x'}, tc.length)...)
			conn.data = makePacket(2, append(payload, 0))
			conn.maxReads = 1
			err := mc.handleAuthResult(t.Context(), []byte("0123456789abcdefghij"), &nativePasswordPlugin{}, newAuthContext(mc.cfg, mc.cfg.Passwd, false))
			if !errors.Is(err, ErrMalformPkt) || conn.writes != 0 {
				t.Fatalf("handleAuthResult = %v, writes = %d; want ErrMalformPkt, 0", err, conn.writes)
			}
		})
	}
}

package mysql

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
)

// Check the actual handshake bytes against explicit protocol encodings,
// including both the JWT length and the enclosing auth-response length.
func TestOpenIDConnectProtocol(t *testing.T) {
	for _, tc := range []struct {
		size   int
		prefix []byte
	}{
		{20, []byte{22, 1, 20}},
		{250, []byte{0xfc, 0xfc, 0, 1, 250}},
		{251, []byte{0xfc, 0xff, 0, 1, 0xfc, 0xfb, 0}},
		{1024, []byte{0xfc, 4, 4, 1, 0xfc, 0, 4}},
	} {
		t.Run(fmt.Sprint(tc.size), func(t *testing.T) {
			conn, mc := newRWMockConn(2)
			mc.capabilities = clientProtocol41 | clientPluginAuth | clientPluginAuthLenEncClientData
			token := strings.Repeat("x", tc.size)
			plugin := &openIDConnectAuthPlugin{token: token}
			response, err := plugin.InitAuth(t.Context(), nil, newAuthContext(mc.cfg, "", true))
			if err != nil {
				t.Fatal(err)
			}
			if err := mc.writeHandshakeResponsePacket(response, openIDConnectPlugin); err != nil {
				t.Fatal(err)
			}
			want := append(append([]byte{}, tc.prefix...), token...)
			want = append(want, openIDConnectPlugin+"\x00"...)
			// Packet header, fixed handshake fields, and empty username.
			const offset = 4 + 32 + 1
			if len(conn.written) < offset || !bytes.Equal(conn.written[offset:], want) {
				t.Fatalf("incorrect OIDC handshake: %x", conn.written)
			}
		})
	}
}

func TestOpenIDConnectRequiresTLS(t *testing.T) {
	plugin := &openIDConnectAuthPlugin{token: "test-token"}
	response, err := plugin.InitAuth(t.Context(), nil, newAuthContext(NewConfig(), "", false))
	if !errors.Is(err, ErrOpenIDConnectTLS) || len(response) != 0 {
		t.Fatalf("InitAuth without TLS = %x, %v", response, err)
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
			conn, mc := newRWMockConn(0)
			mc.cfg.Apply(OIDCToken(token))
			conn.data = makePacket(0, tc.reply)
			err := mc.handleAuthResult(t.Context(), nil, &openIDConnectAuthPlugin{token: token}, newAuthContext(mc.cfg, "", true))
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
			if strings.Contains(err.Error(), token) {
				t.Fatal("server error leaked token")
			}
			if len(conn.written) != 0 {
				t.Fatal("unexpected authentication response")
			}
		})
	}
}

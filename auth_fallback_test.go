package mysql

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
)

func authFallbackSwitch(plugin string) []byte {
	payload := append([]byte{iEOF}, plugin...)
	payload = append(payload, 0)
	payload = append(payload, "0123456789abcdefghij\x00"...)
	return makePacket(2, payload)
}

func TestConnectorGreetingAuthFallback(t *testing.T) {
	for _, tc := range []struct {
		name           string
		greeting       string
		switchPlugin   string
		disableNative  bool
		wantErr        error
		wantWrites     int
		responsePlugin string
	}{
		{name: "unknown default", greeting: "unknown_server_default", switchPlugin: "mysql_native_password", wantWrites: 2, responsePlugin: defaultAuthPlugin},
		{name: "disabled old default", greeting: "mysql_old_password", switchPlugin: "mysql_native_password", wantWrites: 2, responsePlugin: defaultAuthPlugin},
		{name: "disabled cleartext default", greeting: "mysql_clear_password", switchPlugin: "mysql_native_password", wantWrites: 2, responsePlugin: defaultAuthPlugin},
		{name: "fallback disabled", greeting: "unknown_server_default", disableNative: true, wantErr: ErrNativePassword},
		{name: "unknown account plugin", greeting: "mysql_native_password", switchPlugin: "unknown_account_plugin", wantErr: ErrUnknownPlugin, wantWrites: 1, responsePlugin: defaultAuthPlugin},
		{name: "disabled account plugin", greeting: "mysql_native_password", switchPlugin: "mysql_clear_password", wantErr: ErrCleartextPassword, wantWrites: 1, responsePlugin: defaultAuthPlugin},
		{name: "disabled native account", greeting: "caching_sha2_password", switchPlugin: "mysql_native_password", disableNative: true, wantErr: ErrNativePassword, wantWrites: 1, responsePlugin: "caching_sha2_password"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			caps := clientMySQL | clientProtocol41 | clientSecureConn | clientPluginAuth | clientPluginAuthLenEncClientData
			mock := &mockConn{
				data:          authTestHandshake(tc.greeting, caps),
				queuedReplies: [][]byte{authFallbackSwitch(tc.switchPlugin), makePacket(4, []byte{0, 0, 0, 2, 0, 0, 0})},
				maxReads:      3,
			}
			cfg := NewConfig()
			cfg.User, cfg.Passwd = "user", "secret"
			cfg.AllowNativePasswords = !tc.disableNative
			cfg.DialFunc = func(context.Context, string, string) (net.Conn, error) { return mock, nil }
			c, err := NewConnector(cfg)
			if err != nil {
				t.Fatal(err)
			}
			conn, err := c.Connect(t.Context())
			if conn != nil {
				defer conn.Close()
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Connect = %v, want %v", err, tc.wantErr)
			}
			if mock.writes != tc.wantWrites {
				t.Fatalf("writes = %d, want %d", mock.writes, tc.wantWrites)
			}
			if tc.wantWrites > 0 {
				size := int(mock.written[0]) | int(mock.written[1])<<8 | int(mock.written[2])<<16
				if !bytes.HasSuffix(mock.written[4:4+size], append([]byte(tc.responsePlugin), 0)) {
					t.Fatal("HandshakeResponse did not advertise the selected plugin")
				}
			}
		})
	}
}

func TestConnectorDoesNotFallbackAfterPluginError(t *testing.T) {
	for _, want := range []error{errors.New("transport policy rejected"), ErrUnknownPlugin, context.Canceled} {
		t.Run(want.Error(), func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			const name = "test_auth_no_fallback"
			calls := 0
			registerTestAuthPlugin(t, name, func() AuthPlugin {
				return &authTestPlugin{init: func(context.Context, []byte, *AuthContext) ([]byte, error) {
					calls++
					if want == context.Canceled {
						cancel()
						return []byte("must not send"), nil
					}
					return nil, want
				}}
			})
			caps := clientMySQL | clientProtocol41 | clientSecureConn | clientPluginAuth | clientPluginAuthLenEncClientData
			mock := &mockConn{data: authTestHandshake(name, caps), maxReads: 1}
			cfg := NewConfig()
			cfg.DialFunc = func(context.Context, string, string) (net.Conn, error) { return mock, nil }
			c, err := NewConnector(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.Connect(ctx); !errors.Is(err, want) {
				t.Fatalf("Connect = %v, want %v", err, want)
			}
			if calls != 1 || mock.writes != 0 {
				t.Fatalf("plugin calls = %d, writes = %d; want 1, 0", calls, mock.writes)
			}
		})
	}
}

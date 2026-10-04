// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2023 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"context"
	"crypto/rsa"
	"crypto/tls"
	"errors"
	"math/big"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSimpleAuthRejectsContinuation(t *testing.T) {
	nextPacket, err := (simpleAuth{}).ContinuationAuth(context.Background(), nil, nil, &AuthContext{})
	if err != ErrMalformPkt {
		t.Fatalf("expected ErrMalformPkt, got %v", err)
	}
	if nextPacket != nil {
		t.Errorf("expected no response packet, got %v", nextPacket)
	}
}

func TestRegisterAuthPluginFactoryLifetime(t *testing.T) {
	var calls atomic.Int32
	const name = "test_auth_factory_lifetime"
	registerTestAuthPlugin(t, name, func() AuthPlugin {
		calls.Add(1)
		return &authTestPlugin{}
	})
	if calls.Load() != 0 {
		t.Fatal("registration called the factory")
	}
	one, ok := globalPluginRegistry.GetPlugin(name)
	if !ok {
		t.Fatal("plugin was not registered")
	}
	two, ok := globalPluginRegistry.GetPlugin(name)
	if !ok || one == two || calls.Load() != 2 {
		t.Fatal("lookups did not create independent plugin instances")
	}
	RegisterAuthPlugin(name, func() AuthPlugin { return &nativePasswordPlugin{} })
	if plugin, ok := globalPluginRegistry.GetPlugin(name); !ok {
		t.Fatal("replacement was not registered")
	} else if _, ok := plugin.(*nativePasswordPlugin); !ok {
		t.Fatalf("replacement returned %T", plugin)
	}
}

func TestAuthPluginFactoryOutsideLock(t *testing.T) {
	r := newPluginRegistry()
	var calls atomic.Int32
	r.Register("outer", func() AuthPlugin {
		// Factories may consult or update the registry, even when multiple
		// connections start authentication concurrently.
		r.Register("inner", func() AuthPlugin { return &nativePasswordPlugin{} })
		plugin, _ := r.GetPlugin("inner")
		calls.Add(1)
		return plugin
	})
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.GetPlugin("outer")
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		if calls.Load() != 20 {
			t.Fatalf("factory calls = %d, want 20", calls.Load())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("factory was called with the registry locked")
	}
}

func TestAuthContextSnapshot(t *testing.T) {
	cfg := NewConfig()
	cfg.User = "user"
	cfg.Passwd = "first-factor"
	cfg.Net = "unix"
	cfg.TLS = &tls.Config{}
	cfg.pubKey = &rsa.PublicKey{N: big.NewInt(12345), E: 65537}
	auth := newAuthContext(cfg, "selected-factor", false)

	// Later configuration changes must not change an active exchange.
	cfg.User = "changed"
	cfg.Passwd = "changed"
	cfg.Net = "tcp"
	cfg.pubKey.N.SetInt64(99)
	cfg.pubKey.E = 3
	if auth.User() != "user" || auth.Password() != "selected-factor" || !auth.UnixSocket() {
		t.Fatal("authentication snapshot changed with the configuration")
	}
	if auth.TLS() {
		t.Fatal("configured TLS must not be reported as an established TLS connection")
	}
	key := auth.ServerPublicKey()
	if key.N.Int64() != 12345 || key.E != 65537 {
		t.Fatal("authentication snapshot shares the configured RSA key")
	}
	key.N.SetInt64(42)
	key.E = 17
	key = auth.ServerPublicKey()
	if key.N.Int64() != 12345 || key.E != 65537 {
		t.Fatal("ServerPublicKey returned a mutable reference into the snapshot")
	}
	if (&AuthContext{}).ServerPublicKey() != nil {
		t.Fatal("expected nil for an unconfigured RSA key")
	}
}

func TestInitAuthPolicy(t *testing.T) {
	for _, tc := range []struct {
		plugin string
		want   error
	}{
		{"mysql_native_password", ErrNativePassword},
		{"mysql_old_password", ErrOldPassword},
		{"mysql_clear_password", ErrCleartextPassword},
	} {
		t.Run(tc.plugin, func(t *testing.T) {
			_, mc := newRWMockConn(1)
			mc.cfg.AllowNativePasswords = false
			mc.cfg.Passwd = "password"
			auth := newAuthContext(mc.cfg, mc.cfg.Passwd, false)
			plugin, response, err := mc.initAuth(context.Background(), tc.plugin, nil, auth)
			if err != tc.want || plugin != nil || response != nil {
				t.Fatalf("initAuth() = (%T, %v, %v), want (nil, nil, %v)", plugin, response, err, tc.want)
			}
		})
	}
}

func TestAuthPluginCancellation(t *testing.T) {
	for _, stage := range []string{"initial", "continuation"} {
		t.Run(stage, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			entered := make(chan struct{})
			waitForCancel := func(gotCtx context.Context, _ []byte, _ *AuthContext) ([]byte, error) {
				if gotCtx != ctx {
					return nil, errors.New("plugin did not receive the connection context")
				}
				close(entered)
				<-gotCtx.Done()
				return nil, gotCtx.Err()
			}
			const name = "test_auth_cancel"
			factory := func() AuthPlugin {
				return &authTestPlugin{
					init: waitForCancel,
					next: func(ctx context.Context, packet, _ []byte, auth *AuthContext) ([]byte, error) {
						return waitForCancel(ctx, packet, auth)
					},
				}
			}
			registerTestAuthPlugin(t, name, factory)
			conn, mc := newRWMockConn(2)
			auth := newAuthContext(mc.cfg, mc.cfg.Passwd, false)
			conn.data = makePacket(2, []byte{iAuthMoreData, 7})
			conn.maxReads = 1
			result := make(chan error, 1)
			go func() {
				if stage == "initial" {
					_, _, err := mc.initAuth(ctx, name, nil, auth)
					result <- err
				} else {
					result <- mc.handleAuthResult(ctx, nil, factory(), auth)
				}
			}()
			select {
			case <-entered:
				cancel()
			case err := <-result:
				t.Fatalf("plugin returned without waiting for cancellation: %v", err)
			case <-ctx.Done():
				t.Fatal("plugin was not called")
			}
			if err := <-result; err != context.Canceled {
				t.Fatalf("got %v, want context.Canceled", err)
			}
			if len(conn.written) != 0 {
				t.Error("sent authentication data after cancellation")
			}
		})
	}
}

func TestAuthSwitchContextSnapshot(t *testing.T) {
	conn, mc := newRWMockConn(2)
	mc.cfg.User = "user"
	mc.cfg.Passwd = "configured-password"
	auth := newAuthContext(mc.cfg, "selected-password", true)
	// A switch must keep the selected credentials and established transport,
	// even if the original configuration changes.
	mc.cfg.User = "changed"
	mc.cfg.Passwd = "changed"
	ctx := t.Context()
	var switchedAuth *AuthContext
	continued := false
	const name = "test_auth_switch_context"
	registerTestAuthPlugin(t, name, func() AuthPlugin {
		return &authTestPlugin{
			init: func(gotCtx context.Context, seed []byte, gotAuth *AuthContext) ([]byte, error) {
				if gotCtx != ctx || gotAuth == auth || gotAuth.User() != "user" || gotAuth.Password() != "selected-password" || !gotAuth.TLS() || string(seed) != "challenge" {
					return nil, errors.New("auth switch did not preserve the authentication snapshot")
				}
				switchedAuth = gotAuth
				return []byte{42}, nil
			},
			next: func(gotCtx context.Context, packet, seed []byte, gotAuth *AuthContext) ([]byte, error) {
				if gotCtx != ctx || gotAuth != switchedAuth || string(seed) != "challenge" {
					return nil, errors.New("continuation did not retain the switched context")
				}
				continued = true
				return nil, nil // Read the queued OK without sending a response.
			},
		}
	})
	payload := append([]byte{iEOF}, name...)
	payload = append(payload, 0)
	payload = append(payload, "challenge\x00"...)
	conn.data = makePacket(2, payload)
	conn.queuedReplies = [][]byte{append(
		makePacket(4, []byte{iAuthMoreData, 7}),
		makePacket(5, []byte{0, 0, 0, 2, 0, 0, 0})...,
	)}
	conn.maxReads = 2
	if err := mc.handleAuthResult(ctx, nil, &nativePasswordPlugin{}, auth); err != nil {
		t.Fatal(err)
	}
	if !continued || conn.writes != 1 {
		t.Fatalf("continued = %v, writes = %d; want true, 1", continued, conn.writes)
	}
}

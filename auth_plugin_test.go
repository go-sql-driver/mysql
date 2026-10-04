// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2023 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"crypto/tls"
	"testing"
)

type secureTestAuthPlugin struct {
	SimpleAuth
}

func (p *secureTestAuthPlugin) PluginName() string {
	return "secure_test"
}

func (p *secureTestAuthPlugin) InitAuth(authData []byte, cfg *Config) ([]byte, error) {
	return nil, nil
}

func (p *secureTestAuthPlugin) RequireSecure(cfg *Config) bool {
	return true
}

func TestSimpleAuthRejectsContinuation(t *testing.T) {
	nextPacket, err := (SimpleAuth{}).ContinuationAuth(nil, nil, NewConfig())
	if err != ErrMalformPkt {
		t.Fatalf("expected ErrMalformPkt, got %v", err)
	}
	if nextPacket != nil {
		t.Errorf("expected no response packet, got %v", nextPacket)
	}
}

func TestRequireSecureTransport(t *testing.T) {
	plugin := &secureTestAuthPlugin{}

	if err := requireSecureTransport(plugin, &Config{}); err != ErrSecureTransport {
		t.Errorf("expected ErrSecureTransport, got %v", err)
	}
	if err := requireSecureTransport(plugin, &Config{TLS: &tls.Config{}}); err != nil {
		t.Errorf("secure transport over TLS should be allowed, got %v", err)
	}
	if err := requireSecureTransport(plugin, &Config{Net: "unix"}); err != nil {
		t.Errorf("secure transport over unix socket should be allowed, got %v", err)
	}
	if _, ok := any(&ClearPasswordPlugin{}).(SecureTransportRequirer); ok {
		t.Error("cleartext plugin must not require a secure transport")
	}
	if err := requireSecureTransport(&ClearPasswordPlugin{}, &Config{}); err != nil {
		t.Errorf("cleartext plugin should not require a secure transport, got %v", err)
	}
}

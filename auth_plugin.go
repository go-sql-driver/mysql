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
	"sync"
)

// AuthPlugin represents an authentication plugin for MySQL/MariaDB.
// The driver creates an instance for each authentication exchange and calls its
// methods sequentially. ctx is the connection's context and may be canceled
// while a method is running. Plugins should use it to cancel blocking work.
type AuthPlugin interface {
	// InitAuth initializes the authentication process and returns the initial response.
	// authData is the challenge data from the server. It must not be modified
	// and must be copied if retained after the call.
	// auth is the snapshot for this exchange, shared with ContinuationAuth.
	// A nil or empty response sends an empty authentication response.
	// Plugins must check any transport requirements here before returning credentials.
	InitAuth(ctx context.Context, authData []byte, auth *AuthContext) ([]byte, error)

	// ContinuationAuth processes the server's continuation payload after the
	// driver has handled OK, ERR, and auth switch packets and stripped any
	// AuthMoreData prefix. authData is the challenge passed to InitAuth.
	// Neither slice may be modified; copy them if retaining them after the call.
	// A nil response reads the next server packet without sending. A non-nil
	// empty response sends an empty packet. An error aborts authentication.
	// Only a server OK packet completes authentication successfully.
	ContinuationAuth(ctx context.Context, packet, authData []byte, auth *AuthContext) (nextPacket []byte, err error)
}

// simpleAuth provides the default continuation behavior for authentication
// plugins that complete after their initial response. The driver handles OK,
// ERR, and authentication switch packets before dispatching to the plugin, so
// any call to ContinuationAuth represents an unexpected packet.
type simpleAuth struct{}

func (s simpleAuth) ContinuationAuth(ctx context.Context, packet, authData []byte, auth *AuthContext) ([]byte, error) {
	return nil, ErrMalformPkt
}

// pluginRegistry is a registry of available authentication plugins
type pluginRegistry struct {
	mu      sync.RWMutex
	plugins map[string]func() AuthPlugin
}

var globalPluginRegistry = newPluginRegistry()

// newPluginRegistry creates a new plugin registry.
func newPluginRegistry() *pluginRegistry {
	registry := &pluginRegistry{
		plugins: make(map[string]func() AuthPlugin),
	}
	return registry
}

// Register adds a plugin factory to the registry
func (r *pluginRegistry) Register(name string, factory func() AuthPlugin) {
	if factory == nil {
		panic("auth plugin factory cannot be nil")
	}
	r.mu.Lock()
	r.plugins[name] = factory
	r.mu.Unlock()
}

// GetPlugin returns a new plugin instance for the given name
func (r *pluginRegistry) GetPlugin(name string) (AuthPlugin, bool) {
	r.mu.RLock()
	factory, ok := r.plugins[name]
	r.mu.RUnlock()
	if !ok {
		return nil, false
	}
	return factory(), true
}

// RegisterAuthPlugin registers a factory for the server's authentication plugin
// name, replacing any existing registration for that name. It does not call the
// factory. The driver calls factory for each authentication exchange, possibly
// concurrently for different connections. The factory must return a new, non-nil
// AuthPlugin whose mutable state is not shared with other exchanges.
func RegisterAuthPlugin(name string, factory func() AuthPlugin) {
	globalPluginRegistry.Register(name, factory)
}

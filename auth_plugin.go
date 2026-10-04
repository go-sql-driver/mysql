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
	// PluginName returns the name of the authentication plugin
	PluginName() string

	// InitAuth initializes the authentication process and returns the initial response.
	// authData is the challenge data from the server. It must not be modified
	// and must be copied if retained after the call.
	// auth is the snapshot for this exchange, shared with ContinuationAuth.
	// A nil or empty response sends an empty authentication response.
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

// SecureTransportRequirer is an optional interface an AuthPlugin may implement
// to demand that it only run over a secure transport: TLS or a local unix
// socket.
type SecureTransportRequirer interface {
	// RequireSecure reports whether, for the given authentication exchange, the plugin
	// must only be used over a secure transport.
	RequireSecure(auth *AuthContext) bool
}

// SimpleAuth provides the default continuation behavior for authentication
// plugins that complete after their initial response. The driver handles OK,
// ERR, and authentication switch packets before dispatching to the plugin, so
// any call to ContinuationAuth represents an unexpected packet.
type SimpleAuth struct {
	AuthPlugin
}

func (s SimpleAuth) ContinuationAuth(ctx context.Context, packet, authData []byte, auth *AuthContext) ([]byte, error) {
	return nil, ErrMalformPkt
}

// requireSecureTransport returns ErrSecureTransport when plugin opts into
// SecureTransportRequirer and demands a secure transport, but the connection is
// neither using TLS nor a local unix socket. Plugins that do not implement the
// interface are allowed over any transport.
func requireSecureTransport(plugin AuthPlugin, auth *AuthContext) error {
	sr, ok := plugin.(SecureTransportRequirer)
	if !ok || !sr.RequireSecure(auth) {
		return nil
	}
	if !auth.TLS() && !auth.UnixSocket() {
		return ErrSecureTransport
	}
	return nil
}

// pluginRegistry is a registry of available authentication plugins
type pluginRegistry struct {
	mu      sync.RWMutex
	plugins map[string]func() AuthPlugin
}

// newPluginRegistry creates a new plugin registry.
func newPluginRegistry() *pluginRegistry {
	registry := &pluginRegistry{
		plugins: make(map[string]func() AuthPlugin),
	}
	return registry
}

// Register adds a plugin factory to the registry
func (r *pluginRegistry) Register(factory func() AuthPlugin) {
	plugin := factory()
	name := plugin.PluginName()

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

// RegisterAuthPlugin registers the plugin factory to the global plugin registry
func RegisterAuthPlugin(factory func() AuthPlugin) {
	globalPluginRegistry.Register(factory)
}

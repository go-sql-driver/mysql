// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2023 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import "sync"

// AuthPlugin represents an authentication plugin for MySQL/MariaDB
type AuthPlugin interface {
	// PluginName returns the name of the authentication plugin
	PluginName() string

	// InitAuth initializes the authentication process and returns the initial response.
	// authData is the challenge data from the server.
	// cfg is the connection configuration (including the password).
	InitAuth(authData []byte, cfg *Config) ([]byte, error)

	// ContinuationAuth processes the authentication response from the server
	// packet is the data from the server's auth response
	// authData is the initial auth data from the server
	// cfg is the connection configuration
	// Returns:
	//   - nextPacket: the next packet to send to the server (nil if plugin is done processing)
	//   - done: true if authentication processing is complete (OK/ERR/EOF received)
	//   - error: any error that occurred
	ContinuationAuth(packet []byte, authData []byte, cfg *Config) (nextPacket []byte, done bool, err error)
}

// SecureTransportRequirer is an optional interface an AuthPlugin may implement
// to demand that it only run over a secure transport: TLS or a local unix
// socket.
type SecureTransportRequirer interface {
	// RequireSecure reports whether, for the given configuration, the plugin
	// must only be used over a secure transport.
	RequireSecure(cfg *Config) bool
}

type SimpleAuth struct {
	AuthPlugin
}

func (s SimpleAuth) ContinuationAuth(packet []byte, authData []byte, cfg *Config) ([]byte, bool, error) {
	// Simple auth plugins are done after the first packet
	return nil, true, nil
}

// RequireSecure provides the default for plugins embedding SimpleAuth: they do
// not require a secure transport unless they override this method.
func (s SimpleAuth) RequireSecure(cfg *Config) bool {
	return false
}

// requireSecureTransport returns ErrSecureTransport when plugin opts into
// SecureTransportRequirer and demands a secure transport, but the connection is
// neither using TLS nor a local unix socket. Plugins that do not implement the
// interface are allowed over any transport.
func requireSecureTransport(plugin AuthPlugin, cfg *Config) error {
	sr, ok := plugin.(SecureTransportRequirer)
	if !ok || !sr.RequireSecure(cfg) {
		return nil
	}
	if cfg.TLS == nil && cfg.Net != "unix" {
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
	r.mu.Lock()
	defer r.mu.Unlock()
	plugin := factory()
	r.plugins[plugin.PluginName()] = factory
}

// GetPlugin returns a new plugin instance for the given name
func (r *pluginRegistry) GetPlugin(name string) (AuthPlugin, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	factory, ok := r.plugins[name]
	if !ok {
		return nil, false
	}
	return factory(), true
}

// RegisterAuthPlugin registers the plugin factory to the global plugin registry
func RegisterAuthPlugin(factory func() AuthPlugin) {
	globalPluginRegistry.Register(factory)
}

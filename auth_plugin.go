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

	// InitAuth initializes the authentication process and returns the initial response
	// authData is the challenge data from the server
	// password is the password for authentication
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

type SimpleAuth struct {
	AuthPlugin
}

func (s SimpleAuth) ContinuationAuth(packet []byte, authData []byte, cfg *Config) ([]byte, bool, error) {
	// Simple auth plugins are done after the first packet
	return nil, true, nil
}

// pluginRegistry is a registry of available authentication plugins
type pluginRegistry struct {
	mu      sync.RWMutex
	plugins map[string]func() AuthPlugin
}

// NewPluginRegistry creates a new plugin registry
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

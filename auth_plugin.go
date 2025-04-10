// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2023 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

// AuthPlugin represents an authentication plugin for MySQL/MariaDB
type AuthPlugin interface {
	// GetPluginName returns the name of the authentication plugin
	GetPluginName() string

	// InitAuth initializes the authentication process and returns the initial response
	// authData is the challenge data from the server
	// password is the password for authentication
	InitAuth(authData []byte, cfg *Config) ([]byte, error)

	// ProcessAuthResponse processes the authentication response from the server
	// packet is the data from the server's auth response
	// authData is the initial auth data from the server
	// conn is the MySQL connection (for performing additional interactions if needed)
	ProcessAuthResponse(packet []byte, authData []byte, conn *mysqlConn) ([]byte, error)
}

// PluginRegistry is a registry of available authentication plugins
type PluginRegistry struct {
	plugins map[string]AuthPlugin
}

// NewPluginRegistry creates a new plugin registry
func NewPluginRegistry() *PluginRegistry {
	registry := &PluginRegistry{
		plugins: make(map[string]AuthPlugin),
	}
	return registry
}

// Register adds a plugin to the registry
func (r *PluginRegistry) Register(plugin AuthPlugin) {
	r.plugins[plugin.GetPluginName()] = plugin
}

// GetPlugin returns the plugin for the given name
func (r *PluginRegistry) GetPlugin(name string) (AuthPlugin, bool) {
	plugin, ok := r.plugins[name]
	return plugin, ok
}

// RegisterAuthPlugin registers the plugin to the global plugin registry
func RegisterAuthPlugin(plugin AuthPlugin) {
	globalPluginRegistry.Register(plugin)
}

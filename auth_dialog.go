// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2024 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"fmt"
	"strings"
)

const (
	dialogPluginName = "dialog"
)

// dialogAuthPlugin implements the MariaDB PAM authentication plugin
type dialogAuthPlugin struct {
	AuthPlugin
}

func init() {
	RegisterAuthPlugin(&dialogAuthPlugin{})
}

// GetPluginName returns the name of the authentication plugin
func (p *dialogAuthPlugin) GetPluginName() string {
	return dialogPluginName
}

// InitAuth initializes the authentication process
func (p *dialogAuthPlugin) InitAuth(authData []byte, cfg *Config) ([]byte, error) {
	if !cfg.AllowDialogPasswords {
		return nil, ErrDialogAuth
	}
	return append([]byte(cfg.Passwd), 0), nil
}

// ProcessAuthResponse processes the authentication response from the server
func (p *dialogAuthPlugin) ProcessAuthResponse(packet []byte, authData []byte, conn *mysqlConn) ([]byte, error) {

	if len(packet) == 0 {
		return nil, fmt.Errorf("%w: empty auth response packet", ErrMalformPkt)
	}

	switch packet[0] {
	case iOK, iERR, iEOF:
		return packet, nil
	default:
		// Initialize passwords from Config
		var passwords []string
		if conn.cfg.OtherPasswd != "" {
			// Additional passwords from OtherPasswd (comma separated)
			otherPasswords := strings.Split(conn.cfg.OtherPasswd, ",")
			passwords = append(passwords, otherPasswords...)
		}
		currentPasswordIndex := 0
		for {
			var authResp []byte
			if len(passwords) >= currentPasswordIndex+1 {
				authResp = append([]byte(passwords[currentPasswordIndex]), 0)
			} else {
				authResp = []byte{0}
			}
			currentPasswordIndex++

			// Send the authentication response
			if err := conn.writeAuthSwitchPacket(authResp); err != nil {
				return nil, fmt.Errorf("failed to write dialog packet: %w", err)
			}

			// Read the final authentication result
			response, err := conn.readPacket()
			if err != nil {
				return nil, fmt.Errorf("failed to read dialog packet: %w", err)
			}
			if len(response) == 0 {
				return nil, fmt.Errorf("%w: empty auth response packet", ErrMalformPkt)
			}

			switch response[0] {
			case iOK, iERR, iEOF:
				return response, nil
			default:
				continue
			}
		}
	}
}

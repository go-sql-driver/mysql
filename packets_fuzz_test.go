// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2026 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"crypto/tls"
	"testing"
)

func FuzzReadHandshakePacket(f *testing.F) {
	full := testHandshakePacket()
	for n := 0; n <= len(full); n++ {
		f.Add(full[:n], false)
		f.Add(full[:n], true)
	}
	for _, payload := range [][]byte{{10, 0}, {10, 'a', 'b'}, {iERR}, {iERR, 1}, {iERR, 1, 0, '#'}} {
		f.Add(payload, false)
	}
	f.Fuzz(func(t *testing.T, payload []byte, useTLS bool) {
		if len(payload) > 4096 {
			t.Skip("handshake too large for this fuzz target")
		}
		conn, mc := newRWMockConn(0)
		mc.rawConn = conn
		mc.cfg.Logger = &NopLogger{}
		if useTLS {
			mc.cfg.TLS = &tls.Config{}
		}
		conn.data = makePacket(0, payload)
		conn.maxReads = 1
		// All server-controlled bytes must be safe to parse, including ERR
		// packets and the greeting that precedes TLS negotiation.
		mc.readHandshakePacket()
	})
}

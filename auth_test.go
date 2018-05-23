// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2018 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"bytes"
	"fmt"
	"testing"
)

func TestScrambleOldPass(t *testing.T) {
	scramble := []byte{9, 8, 7, 6, 5, 4, 3, 2}
	vectors := []struct {
		pass string
		out  string
	}{
		{" pass", "47575c5a435b4251"},
		{"pass ", "47575c5a435b4251"},
		{"123\t456", "575c47505b5b5559"},
		{"C0mpl!ca ted#PASS123", "5d5d554849584a45"},
	}
	for _, tuple := range vectors {
		ours := scrambleOldPassword(scramble, tuple.pass)
		if tuple.out != fmt.Sprintf("%x", ours) {
			t.Errorf("Failed old password %q", tuple.pass)
		}
	}
}

func TestScrambleSHA256Pass(t *testing.T) {
	scramble := []byte{10, 47, 74, 111, 75, 73, 34, 48, 88, 76, 114, 74, 37, 13, 3, 80, 82, 2, 23, 21}
	vectors := []struct {
		pass string
		out  string
	}{
		{"secret", "f490e76f66d9d86665ce54d98c78d0acfe2fb0b08b423da807144873d30b312c"},
		{"secret2", "abc3934a012cf342e876071c8ee202de51785b430258a7a0138bc79c4d800bc6"},
	}
	for _, tuple := range vectors {
		ours := scrambleSHA256Password(scramble, tuple.pass)
		if tuple.out != fmt.Sprintf("%x", ours) {
			t.Errorf("Failed SHA256 password %q", tuple.pass)
		}
	}

}

func TestAuthSwitchOldPasswordNotAllowed(t *testing.T) {
	conn, mc := newRWMockConn(2)

	conn.data = []byte{41, 0, 0, 2, 254, 109, 121, 115, 113, 108, 95, 111, 108,
		100, 95, 112, 97, 115, 115, 119, 111, 114, 100, 0, 95, 84, 103, 43, 61,
		49, 123, 61, 91, 50, 40, 113, 35, 84, 96, 101, 92, 123, 121, 107, 0}
	conn.maxReads = 1
	authData := []byte{95, 84, 103, 43, 61, 49, 123, 61, 91, 50, 40, 113, 35,
		84, 96, 101, 92, 123, 121, 107}
	plugin := "mysql_native_password"
	err := mc.handleAuthResult(authData, plugin)
	if err != ErrOldPassword {
		t.Errorf("expected ErrOldPassword, got %v", err)
	}
}

func TestAuthSwitchOldPassword(t *testing.T) {
	conn, mc := newRWMockConn(2)
	mc.cfg.AllowOldPasswords = true
	mc.cfg.Passwd = "secret"

	// auth switch request
	conn.data = []byte{41, 0, 0, 2, 254, 109, 121, 115, 113, 108, 95, 111, 108,
		100, 95, 112, 97, 115, 115, 119, 111, 114, 100, 0, 95, 84, 103, 43, 61,
		49, 123, 61, 91, 50, 40, 113, 35, 84, 96, 101, 92, 123, 121, 107, 0}

	// auth response
	conn.queuedReplies = [][]byte{{8, 0, 0, 4, 0, 0, 0, 2, 0, 0, 0, 0}}
	conn.maxReads = 2

	authData := []byte{95, 84, 103, 43, 61, 49, 123, 61, 91, 50, 40, 113, 35,
		84, 96, 101, 92, 123, 121, 107}
	plugin := "mysql_native_password"

	if err := mc.handleAuthResult(authData, plugin); err != nil {
		t.Errorf("got error: %v", err)
	}

	expectedReply := []byte{8, 0, 0, 3, 86, 83, 83, 79, 74, 78, 65, 66}
	if !bytes.Equal(conn.written, expectedReply) {
		t.Errorf("got unexpected data: %v", conn.written)
	}
}

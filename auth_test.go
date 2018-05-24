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
	"crypto/tls"
	"fmt"
	"testing"
)

var serverPubKey = []byte{1, 45, 45, 45, 45, 45, 66, 69, 71, 73, 78, 32, 80, 85,
	66, 76, 73, 67, 32, 75, 69, 89, 45, 45, 45, 45, 45, 10, 77, 73, 73, 66, 73,
	106, 65, 78, 66, 103, 107, 113, 104, 107, 105, 71, 57, 119, 48, 66, 65, 81,
	69, 70, 65, 65, 79, 67, 65, 81, 56, 65, 77, 73, 73, 66, 67, 103, 75, 67, 65,
	81, 69, 65, 51, 72, 115, 120, 83, 53, 80, 47, 72, 97, 88, 80, 118, 109, 51,
	109, 50, 65, 68, 110, 10, 98, 117, 54, 71, 81, 102, 112, 83, 71, 111, 55,
	104, 50, 103, 104, 56, 49, 112, 109, 97, 120, 107, 67, 110, 68, 67, 119,
	102, 54, 109, 109, 101, 72, 55, 76, 75, 104, 115, 110, 89, 110, 78, 52, 81,
	48, 99, 122, 49, 81, 69, 47, 98, 104, 100, 80, 117, 54, 106, 115, 43, 86,
	97, 89, 52, 10, 67, 99, 77, 117, 98, 80, 78, 49, 103, 79, 75, 97, 89, 118,
	78, 99, 103, 69, 87, 112, 116, 73, 67, 105, 50, 88, 84, 116, 116, 66, 55,
	117, 104, 43, 118, 67, 77, 106, 76, 118, 106, 65, 77, 100, 54, 47, 68, 109,
	120, 100, 98, 85, 66, 48, 122, 80, 71, 113, 68, 79, 103, 105, 76, 68, 10,
	75, 82, 79, 79, 53, 113, 100, 55, 115, 104, 98, 55, 49, 82, 47, 88, 74, 69,
	70, 118, 76, 120, 71, 88, 69, 70, 48, 90, 116, 104, 72, 101, 78, 111, 57,
	102, 69, 118, 120, 70, 81, 111, 109, 98, 49, 107, 90, 57, 74, 56, 110, 66,
	119, 116, 101, 53, 83, 70, 53, 89, 108, 113, 86, 50, 10, 66, 66, 53, 113,
	108, 97, 122, 43, 51, 81, 83, 78, 118, 109, 67, 49, 105, 87, 102, 108, 106,
	88, 98, 89, 53, 107, 51, 47, 97, 54, 109, 107, 77, 47, 76, 97, 87, 104, 97,
	117, 78, 53, 80, 82, 51, 115, 67, 120, 53, 85, 117, 49, 77, 102, 100, 115,
	86, 105, 107, 53, 102, 88, 77, 77, 10, 100, 120, 107, 102, 70, 43, 88, 51,
	99, 104, 107, 65, 110, 119, 73, 51, 70, 117, 119, 119, 50, 87, 71, 109, 87,
	79, 71, 98, 75, 116, 109, 73, 101, 85, 109, 51, 98, 73, 82, 109, 100, 70,
	85, 113, 97, 108, 81, 105, 70, 104, 113, 101, 90, 50, 105, 107, 106, 104,
	103, 86, 73, 57, 112, 76, 10, 119, 81, 73, 68, 65, 81, 65, 66, 10, 45, 45,
	45, 45, 45, 69, 78, 68, 32, 80, 85, 66, 76, 73, 67, 32, 75, 69, 89, 45, 45,
	45, 45, 45, 10}

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

func TestAuthCachingSHA256PasswordCached(t *testing.T) {
	conn, mc := newRWMockConn(1)
	mc.cfg.User = "root"
	mc.cfg.Passwd = "secret"

	authData := []byte{90, 105, 74, 126, 30, 48, 37, 56, 3, 23, 115, 127, 69,
		22, 41, 84, 32, 123, 43, 118}
	plugin := "caching_sha2_password"

	// Send Client Authentication Packet
	authResp, err := mc.auth(authData, plugin)
	if err != nil {
		t.Fatal(err)
	}
	if err = mc.writeHandshakeResponsePacket(authResp, plugin); err != nil {
		t.Fatal(err)
	}

	// check written auth response
	authRespStart := 4 + 4 + 4 + 1 + 23 + len(mc.cfg.User) + 1
	authRespEnd := authRespStart + 1 + len(authResp)
	writtenAuthRespLen := conn.written[authRespStart]
	writtenAuthResp := conn.written[authRespStart+1 : authRespEnd]
	expectedAuthResp := []byte{102, 32, 5, 35, 143, 161, 140, 241, 171, 232, 56,
		139, 43, 14, 107, 196, 249, 170, 147, 60, 220, 204, 120, 178, 214, 15,
		184, 150, 26, 61, 57, 235}
	if writtenAuthRespLen != 32 || !bytes.Equal(writtenAuthResp, expectedAuthResp) {
		t.Fatalf("unexpected written auth response (%d bytes): %v", writtenAuthRespLen, writtenAuthResp)
	}
	conn.written = nil

	// auth response
	conn.data = []byte{
		2, 0, 0, 2, 1, 3, // Fast Auth Success
		7, 0, 0, 3, 0, 0, 0, 2, 0, 0, 0, // OK
	}
	conn.maxReads = 1

	// Handle response to auth packet
	if err := mc.handleAuthResult(authData, plugin); err != nil {
		t.Errorf("got error: %v", err)
	}
}

func TestAuthCachingSHA256PasswordEmpty(t *testing.T) {
	conn, mc := newRWMockConn(1)
	mc.cfg.User = "root"
	mc.cfg.Passwd = ""

	authData := []byte{90, 105, 74, 126, 30, 48, 37, 56, 3, 23, 115, 127, 69,
		22, 41, 84, 32, 123, 43, 118}
	plugin := "caching_sha2_password"

	// Send Client Authentication Packet
	authResp, err := mc.auth(authData, plugin)
	if err != nil {
		t.Fatal(err)
	}
	if err = mc.writeHandshakeResponsePacket(authResp, plugin); err != nil {
		t.Fatal(err)
	}

	// check written auth response
	authRespStart := 4 + 4 + 4 + 1 + 23 + len(mc.cfg.User) + 1
	authRespEnd := authRespStart + 1 + len(authResp)
	writtenAuthRespLen := conn.written[authRespStart]
	writtenAuthResp := conn.written[authRespStart+1 : authRespEnd]
	expectedAuthResp := []byte{}
	if writtenAuthRespLen != 0 || !bytes.Equal(writtenAuthResp, expectedAuthResp) {
		t.Fatalf("unexpected written auth response (%d bytes): %v", writtenAuthRespLen, writtenAuthResp)
	}
	conn.written = nil

	// auth response
	conn.data = []byte{
		7, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0, // OK
	}
	conn.maxReads = 1

	// Handle response to auth packet
	if err := mc.handleAuthResult(authData, plugin); err != nil {
		t.Errorf("got error: %v", err)
	}
}

func TestAuthCachingSHA256PasswordFullRSA(t *testing.T) {
	conn, mc := newRWMockConn(1)
	mc.cfg.User = "root"
	mc.cfg.Passwd = "secret"

	authData := []byte{6, 81, 96, 114, 14, 42, 50, 30, 76, 47, 1, 95, 126, 81,
		62, 94, 83, 80, 52, 85}
	plugin := "caching_sha2_password"

	// Send Client Authentication Packet
	authResp, err := mc.auth(authData, plugin)
	if err != nil {
		t.Fatal(err)
	}
	if err = mc.writeHandshakeResponsePacket(authResp, plugin); err != nil {
		t.Fatal(err)
	}

	// check written auth response
	authRespStart := 4 + 4 + 4 + 1 + 23 + len(mc.cfg.User) + 1
	authRespEnd := authRespStart + 1 + len(authResp)
	writtenAuthRespLen := conn.written[authRespStart]
	writtenAuthResp := conn.written[authRespStart+1 : authRespEnd]
	expectedAuthResp := []byte{171, 201, 138, 146, 89, 159, 11, 170, 0, 67, 165,
		49, 175, 94, 218, 68, 177, 109, 110, 86, 34, 33, 44, 190, 67, 240, 70,
		110, 40, 139, 124, 41}
	if writtenAuthRespLen != 32 || !bytes.Equal(writtenAuthResp, expectedAuthResp) {
		t.Fatalf("unexpected written auth response (%d bytes): %v", writtenAuthRespLen, writtenAuthResp)
	}
	conn.written = nil

	// auth response
	conn.data = []byte{
		2, 0, 0, 2, 1, 4, // Perform Full Authentication
	}
	conn.queuedReplies = [][]byte{
		// pub key response
		append([]byte{byte(len(serverPubKey)), 1, 0, 4}, serverPubKey...),

		// OK
		{7, 0, 0, 6, 0, 0, 0, 2, 0, 0, 0},
	}
	conn.maxReads = 3

	// Handle response to auth packet
	if err := mc.handleAuthResult(authData, plugin); err != nil {
		t.Errorf("got error: %v", err)
	}

	if !bytes.HasPrefix(conn.written, []byte{1, 0, 0, 3, 2, 0, 1, 0, 5}) {
		t.Errorf("unexpected written data: %v", conn.written)
	}
}

func TestAuthCachingSHA256PasswordFullSecure(t *testing.T) {
	conn, mc := newRWMockConn(1)
	mc.cfg.User = "root"
	mc.cfg.Passwd = "secret"

	authData := []byte{6, 81, 96, 114, 14, 42, 50, 30, 76, 47, 1, 95, 126, 81,
		62, 94, 83, 80, 52, 85}
	plugin := "caching_sha2_password"

	// Send Client Authentication Packet
	authResp, err := mc.auth(authData, plugin)
	if err != nil {
		t.Fatal(err)
	}
	if err = mc.writeHandshakeResponsePacket(authResp, plugin); err != nil {
		t.Fatal(err)
	}

	// Hack to make the caching_sha2_password plugin believe that the connection
	// is secure
	mc.cfg.tls = &tls.Config{InsecureSkipVerify: true}

	// check written auth response
	authRespStart := 4 + 4 + 4 + 1 + 23 + len(mc.cfg.User) + 1
	authRespEnd := authRespStart + 1 + len(authResp)
	writtenAuthRespLen := conn.written[authRespStart]
	writtenAuthResp := conn.written[authRespStart+1 : authRespEnd]
	expectedAuthResp := []byte{171, 201, 138, 146, 89, 159, 11, 170, 0, 67, 165,
		49, 175, 94, 218, 68, 177, 109, 110, 86, 34, 33, 44, 190, 67, 240, 70,
		110, 40, 139, 124, 41}
	if writtenAuthRespLen != 32 || !bytes.Equal(writtenAuthResp, expectedAuthResp) {
		t.Fatalf("unexpected written auth response (%d bytes): %v", writtenAuthRespLen, writtenAuthResp)
	}
	conn.written = nil

	// auth response
	conn.data = []byte{
		2, 0, 0, 2, 1, 4, // Perform Full Authentication
	}
	conn.queuedReplies = [][]byte{
		// OK
		{7, 0, 0, 4, 0, 0, 0, 2, 0, 0, 0},
	}
	conn.maxReads = 3

	// Handle response to auth packet
	if err := mc.handleAuthResult(authData, plugin); err != nil {
		t.Errorf("got error: %v", err)
	}

	if !bytes.Equal(conn.written, []byte{6, 0, 0, 3, 115, 101, 99, 114, 101, 116}) {
		t.Errorf("unexpected written data: %v", conn.written)
	}
}

func TestAuthSwitchCleartextPasswordNotAllowed(t *testing.T) {
	conn, mc := newRWMockConn(2)

	conn.data = []byte{22, 0, 0, 2, 254, 109, 121, 115, 113, 108, 95, 99, 108,
		101, 97, 114, 95, 112, 97, 115, 115, 119, 111, 114, 100, 0}
	conn.maxReads = 1
	authData := []byte{123, 87, 15, 84, 20, 58, 37, 121, 91, 117, 51, 24, 19,
		47, 43, 9, 41, 112, 67, 110}
	plugin := "mysql_native_password"
	err := mc.handleAuthResult(authData, plugin)
	if err != ErrCleartextPassword {
		t.Errorf("expected ErrCleartextPassword, got %v", err)
	}
}

func TestAuthSwitchCleartextPassword(t *testing.T) {
	conn, mc := newRWMockConn(2)
	mc.cfg.AllowCleartextPasswords = true
	mc.cfg.Passwd = "secret"

	// auth switch request
	conn.data = []byte{22, 0, 0, 2, 254, 109, 121, 115, 113, 108, 95, 99, 108,
		101, 97, 114, 95, 112, 97, 115, 115, 119, 111, 114, 100, 0}

	// auth response
	conn.queuedReplies = [][]byte{{7, 0, 0, 4, 0, 0, 0, 2, 0, 0, 0}}
	conn.maxReads = 2

	authData := []byte{123, 87, 15, 84, 20, 58, 37, 121, 91, 117, 51, 24, 19,
		47, 43, 9, 41, 112, 67, 110}
	plugin := "mysql_native_password"

	if err := mc.handleAuthResult(authData, plugin); err != nil {
		t.Errorf("got error: %v", err)
	}

	expectedReply := []byte{6, 0, 0, 3, 115, 101, 99, 114, 101, 116}
	if !bytes.Equal(conn.written, expectedReply) {
		t.Errorf("got unexpected data: %v", conn.written)
	}
}

func TestAuthSwitchCleartextPasswordEmpty(t *testing.T) {
	conn, mc := newRWMockConn(2)
	mc.cfg.AllowCleartextPasswords = true
	mc.cfg.Passwd = ""

	// auth switch request
	conn.data = []byte{22, 0, 0, 2, 254, 109, 121, 115, 113, 108, 95, 99, 108,
		101, 97, 114, 95, 112, 97, 115, 115, 119, 111, 114, 100, 0}

	// auth response
	conn.queuedReplies = [][]byte{{7, 0, 0, 4, 0, 0, 0, 2, 0, 0, 0}}
	conn.maxReads = 2

	authData := []byte{123, 87, 15, 84, 20, 58, 37, 121, 91, 117, 51, 24, 19,
		47, 43, 9, 41, 112, 67, 110}
	plugin := "mysql_native_password"

	if err := mc.handleAuthResult(authData, plugin); err != nil {
		t.Errorf("got error: %v", err)
	}

	expectedReply := []byte{0, 0, 0, 3}
	if !bytes.Equal(conn.written, expectedReply) {
		t.Errorf("got unexpected data: %v", conn.written)
	}
}

func TestAuthSwitchNativePasswordNotAllowed(t *testing.T) {
	conn, mc := newRWMockConn(2)
	mc.cfg.AllowNativePasswords = false

	conn.data = []byte{44, 0, 0, 2, 254, 109, 121, 115, 113, 108, 95, 110, 97,
		116, 105, 118, 101, 95, 112, 97, 115, 115, 119, 111, 114, 100, 0, 96,
		71, 63, 8, 1, 58, 75, 12, 69, 95, 66, 60, 117, 31, 48, 31, 89, 39, 55,
		31, 0}
	conn.maxReads = 1
	authData := []byte{96, 71, 63, 8, 1, 58, 75, 12, 69, 95, 66, 60, 117, 31,
		48, 31, 89, 39, 55, 31}
	plugin := "caching_sha2_password"
	err := mc.handleAuthResult(authData, plugin)
	if err != ErrNativePassword {
		t.Errorf("expected ErrNativePassword, got %v", err)
	}
}

func TestAuthSwitchNativePassword(t *testing.T) {
	conn, mc := newRWMockConn(2)
	mc.cfg.AllowNativePasswords = true
	mc.cfg.Passwd = "secret"

	// auth switch request
	conn.data = []byte{44, 0, 0, 2, 254, 109, 121, 115, 113, 108, 95, 110, 97,
		116, 105, 118, 101, 95, 112, 97, 115, 115, 119, 111, 114, 100, 0, 96,
		71, 63, 8, 1, 58, 75, 12, 69, 95, 66, 60, 117, 31, 48, 31, 89, 39, 55,
		31, 0}

	// auth response
	conn.queuedReplies = [][]byte{{7, 0, 0, 4, 0, 0, 0, 2, 0, 0, 0}}
	conn.maxReads = 2

	authData := []byte{96, 71, 63, 8, 1, 58, 75, 12, 69, 95, 66, 60, 117, 31,
		48, 31, 89, 39, 55, 31}
	plugin := "caching_sha2_password"

	if err := mc.handleAuthResult(authData, plugin); err != nil {
		t.Errorf("got error: %v", err)
	}

	expectedReply := []byte{20, 0, 0, 3, 202, 41, 195, 164, 34, 226, 49, 103,
		21, 211, 167, 199, 227, 116, 8, 48, 57, 71, 149, 146}
	if !bytes.Equal(conn.written, expectedReply) {
		t.Errorf("got unexpected data: %v", conn.written)
	}
}

func TestAuthSwitchNativePasswordEmpty(t *testing.T) {
	conn, mc := newRWMockConn(2)
	mc.cfg.AllowNativePasswords = true
	mc.cfg.Passwd = ""

	// auth switch request
	conn.data = []byte{44, 0, 0, 2, 254, 109, 121, 115, 113, 108, 95, 110, 97,
		116, 105, 118, 101, 95, 112, 97, 115, 115, 119, 111, 114, 100, 0, 96,
		71, 63, 8, 1, 58, 75, 12, 69, 95, 66, 60, 117, 31, 48, 31, 89, 39, 55,
		31, 0}

	// auth response
	conn.queuedReplies = [][]byte{{7, 0, 0, 4, 0, 0, 0, 2, 0, 0, 0}}
	conn.maxReads = 2

	authData := []byte{96, 71, 63, 8, 1, 58, 75, 12, 69, 95, 66, 60, 117, 31,
		48, 31, 89, 39, 55, 31}
	plugin := "caching_sha2_password"

	if err := mc.handleAuthResult(authData, plugin); err != nil {
		t.Errorf("got error: %v", err)
	}

	expectedReply := []byte{0, 0, 0, 3}
	if !bytes.Equal(conn.written, expectedReply) {
		t.Errorf("got unexpected data: %v", conn.written)
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

func TestAuthSwitchOldPasswordEmpty(t *testing.T) {
	conn, mc := newRWMockConn(2)
	mc.cfg.AllowOldPasswords = true
	mc.cfg.Passwd = ""

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

	expectedReply := []byte{0, 0, 0, 3}
	if !bytes.Equal(conn.written, expectedReply) {
		t.Errorf("got unexpected data: %v", conn.written)
	}
}
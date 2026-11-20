// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2026 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"bytes"
	"crypto/ed25519"
	"database/sql"
	"encoding/hex"
	"errors"
	"os"
	"testing"
)

func TestParsecAuth(t *testing.T) {
	conn, mc := newRWMockConn(2)
	mc.cfg.Passwd = "secret"

	// auth switch request: parsec with a 32-byte nonce ending with 0x00
	serverNonce := append(bytes.Repeat([]byte{'n'}, 31), 0)
	payload := append([]byte{iEOF}, "parsec"...)
	payload = append(payload, 0)
	payload = append(payload, serverNonce...)
	conn.data = makePacket(2, payload)

	// extended salt (prefixed with 0x01), then OK
	salt := []byte("0123456789abcdefgh")
	extSalt := append([]byte{iAuthMoreData, 'P', 3}, salt...)
	conn.queuedReplies = [][]byte{
		makePacket(4, extSalt),
		{7, 0, 0, 6, 0, 0, 0, 2, 0, 0, 0},
	}
	conn.maxReads = 3

	auth := newAuthContext(mc.cfg, mc.cfg.Passwd, false)
	if err := mc.handleAuthResult(t.Context(), nil, &nativePasswordPlugin{}, auth); err != nil {
		t.Fatalf("got error: %v", err)
	}

	// empty packet requesting the extended salt, then client nonce + signature
	if len(conn.written) != 4+4+96 || !bytes.Equal(conn.written[:8], []byte{0, 0, 0, 3, 96, 0, 0, 5}) {
		t.Fatalf("unexpected packets written: %v", conn.written)
	}
	resp := conn.written[8:]
	clientNonce, signature := resp[:32], resp[32:]

	// Ed25519 public key for password "secret", the salt above and 8192 rounds.
	pub, err := hex.DecodeString("8ee83e3378590a471e45106351302b09aa23e35a3c932e5233033336a05431c2")
	if err != nil {
		t.Fatal(err)
	}
	msg := append(bytes.Clone(serverNonce), clientNonce...)
	if !ed25519.Verify(pub, msg, signature) {
		t.Error("signature does not verify against the PBKDF2-derived key")
	}
}

func TestParsecAuthRejectsMalformed(t *testing.T) {
	auth := newAuthContext(NewConfig(), "secret", false)
	ctx := t.Context()
	nonce := make([]byte, 32)
	saltPacket := append([]byte{'P', 0}, "0123456789abcdefgh"...)

	p := &parsecPlugin{}
	for _, size := range []int{31, 33} {
		if _, err := p.InitAuth(ctx, make([]byte, size), auth); !errors.Is(err, ErrMalformPkt) {
			t.Errorf("%d-byte nonce: got %v, want ErrMalformPkt", size, err)
		}
	}
	if _, err := p.ContinuationAuth(ctx, saltPacket, nonce, auth); !errors.Is(err, ErrMalformPkt) {
		t.Errorf("continuation before init: got %v, want ErrMalformPkt", err)
	}

	for _, tc := range []struct {
		name   string
		packet []byte
	}{
		{"empty", nil},
		{"short salt", saltPacket[:len(saltPacket)-1]},
		{"long salt", append(bytes.Clone(saltPacket), 0)},
		{"wrong KDF", append([]byte{'Q'}, saltPacket[1:]...)},
	} {
		if _, err := p.InitAuth(ctx, nonce, auth); err != nil {
			t.Fatal(err)
		}
		if _, err := p.ContinuationAuth(ctx, tc.packet, nonce, auth); !errors.Is(err, ErrMalformPkt) {
			t.Errorf("%s: got %v, want ErrMalformPkt", tc.name, err)
		}
	}

	if _, err := p.InitAuth(ctx, nonce, auth); err != nil {
		t.Fatal(err)
	}
	if _, err := p.ContinuationAuth(ctx, saltPacket, nonce, auth); err != nil {
		t.Fatal(err)
	}
	if _, err := p.ContinuationAuth(ctx, saltPacket, nonce, auth); !errors.Is(err, ErrMalformPkt) {
		t.Errorf("second continuation: got %v, want ErrMalformPkt", err)
	}

	if _, err := p.InitAuth(ctx, nonce, auth); err != nil {
		t.Fatal(err)
	}
	saltPacket[1] = 4
	if _, err := p.ContinuationAuth(ctx, saltPacket, nonce, auth); err == nil {
		t.Error("accepted unsupported iteration factor 4")
	}
}

func TestParsecAuthServer(t *testing.T) {
	dsn := os.Getenv("MYSQL_PARSEC_TEST_DSN")
	if dsn == "" {
		t.Skip("set MYSQL_PARSEC_TEST_DSN to test a MariaDB PARSEC account")
	}
	cfg, err := ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	connect := func() error {
		db, err := sql.Open(driverNameTest, cfg.FormatDSN())
		if err != nil {
			return err
		}
		defer db.Close()
		return db.PingContext(t.Context())
	}
	if err := connect(); err != nil {
		t.Fatalf("parsec authentication failed: %v", err)
	}
	cfg.Passwd += "!"
	var mysqlErr *MySQLError
	if err := connect(); !errors.As(err, &mysqlErr) || mysqlErr.Number != 1045 {
		t.Errorf("wrong password: got %v, want access denied", err)
	}
}

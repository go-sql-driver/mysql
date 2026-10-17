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
	"context"
	"crypto/ed25519"
	"crypto/pbkdf2"
	"crypto/sha512"
	"errors"
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
	extSalt := append([]byte{iAuthMoreData, 'P', 2}, salt...)
	conn.queuedReplies = [][]byte{
		makePacket(4, extSalt),
		{7, 0, 0, 6, 0, 0, 0, 2, 0, 0, 0},
	}
	conn.maxReads = 3

	auth := newAuthContext(mc.cfg, mc.cfg.Passwd, false)
	if err := mc.handleAuthResult(context.Background(), nil, &nativePasswordPlugin{}, auth); err != nil {
		t.Fatalf("got error: %v", err)
	}

	// empty packet requesting the extended salt, then client nonce + signature
	if len(conn.written) != 4+4+96 || !bytes.Equal(conn.written[:8], []byte{0, 0, 0, 3, 96, 0, 0, 5}) {
		t.Fatalf("unexpected packets written: %v", conn.written)
	}
	resp := conn.written[8:]
	clientNonce, signature := resp[:32], resp[32:]

	seed, err := pbkdf2.Key(sha512.New, "secret", salt, 1024<<2, ed25519.SeedSize)
	if err != nil {
		t.Fatal(err)
	}
	pub := ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)
	msg := append(append([]byte{}, serverNonce...), clientNonce...)
	if !ed25519.Verify(pub, msg, signature) {
		t.Error("signature does not verify against the PBKDF2-derived key")
	}
}

func TestParsecAuthRejectsMalformed(t *testing.T) {
	cfg := NewConfig()
	cfg.Passwd = "secret"
	auth := newAuthContext(cfg, cfg.Passwd, false)
	ctx := context.Background()
	nonce := bytes.Repeat([]byte{'n'}, 32)

	if _, err := (&parsecPlugin{}).InitAuth(ctx, nonce[:31], auth); !errors.Is(err, ErrMalformPkt) {
		t.Errorf("31-byte nonce: got %v, want ErrMalformPkt", err)
	}
	if _, err := (&parsecPlugin{}).ContinuationAuth(ctx, []byte{'P', 0, 's'}, nonce, auth); !errors.Is(err, ErrMalformPkt) {
		t.Errorf("continuation before init: got %v, want ErrMalformPkt", err)
	}

	for _, tc := range []struct {
		name   string
		packet []byte
	}{
		{"wrong KDF", []byte{'Q', 0, 's', 'a', 'l', 't'}},
		{"no salt", []byte{'P', 0}},
		{"empty", nil},
	} {
		p := &parsecPlugin{}
		if _, err := p.InitAuth(ctx, nonce, auth); err != nil {
			t.Fatal(err)
		}
		if _, err := p.ContinuationAuth(ctx, tc.packet, nonce, auth); !errors.Is(err, ErrMalformPkt) {
			t.Errorf("%s: got %v, want ErrMalformPkt", tc.name, err)
		}
	}

	// iteration factor above the limit is refused before any hashing
	p := &parsecPlugin{}
	if _, err := p.InitAuth(ctx, nonce, auth); err != nil {
		t.Fatal(err)
	}
	_, err := p.ContinuationAuth(ctx, []byte{'P', parsecMaxIterationFactor + 1, 's', 'a', 'l', 't'}, nonce, auth)
	if err == nil || errors.Is(err, ErrMalformPkt) {
		t.Errorf("factor %d: got %v, want iteration factor error", parsecMaxIterationFactor+1, err)
	}

	// a second continuation after the response is unexpected
	p = &parsecPlugin{}
	if _, err := p.InitAuth(ctx, nonce, auth); err != nil {
		t.Fatal(err)
	}
	if _, err := p.ContinuationAuth(ctx, []byte{'P', 0, 's', 'a', 'l', 't'}, nonce, auth); err != nil {
		t.Fatal(err)
	}
	if _, err := p.ContinuationAuth(ctx, []byte{'P', 0, 's', 'a', 'l', 't'}, nonce, auth); !errors.Is(err, ErrMalformPkt) {
		t.Errorf("second continuation: got %v, want ErrMalformPkt", err)
	}
}

func TestParsecAuthMaxIterationFactor(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping PBKDF2 at the maximum iteration factor in short mode")
	}
	cfg := NewConfig()
	cfg.Passwd = "secret"
	auth := newAuthContext(cfg, cfg.Passwd, false)
	nonce := bytes.Repeat([]byte{'n'}, 32)
	p := &parsecPlugin{}
	if _, err := p.InitAuth(context.Background(), nonce, auth); err != nil {
		t.Fatal(err)
	}
	resp, err := p.ContinuationAuth(context.Background(), []byte{'P', parsecMaxIterationFactor, 's', 'a', 'l', 't'}, nonce, auth)
	if err != nil || len(resp) != 96 {
		t.Fatalf("factor %d: got %d bytes, %v", parsecMaxIterationFactor, len(resp), err)
	}
}

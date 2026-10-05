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
	"crypto/sha512"

	"filippo.io/edwards25519"
)

// clientEd25519Plugin implements the client_ed25519 authentication
type clientEd25519Plugin struct {
	simpleAuth
}

func init() {
	RegisterAuthPlugin("client_ed25519", func() AuthPlugin { return &clientEd25519Plugin{} })
}

func (p *clientEd25519Plugin) InitAuth(ctx context.Context, authData []byte, auth *AuthContext) ([]byte, error) {
	if len(authData) != 32 {
		return nil, ErrMalformPkt
	}
	// Derived from https://github.com/MariaDB/server/blob/d8e6bb00888b1f82c031938f4c8ac5d97f6874c3/plugin/auth_ed25519/ref10/sign.c
	// Code style is from https://cs.opensource.google/go/go/+/refs/tags/go1.21.5:src/crypto/ed25519/ed25519.go;l=207
	h := sha512.Sum512([]byte(auth.Password()))

	s, err := edwards25519.NewScalar().SetBytesWithClamping(h[:32])
	if err != nil {
		return nil, err
	}
	A := (&edwards25519.Point{}).ScalarBaseMult(s)

	mh := sha512.New()
	mh.Write(h[32:])
	mh.Write(authData)
	messageDigest := mh.Sum(nil)
	r, err := edwards25519.NewScalar().SetUniformBytes(messageDigest)
	if err != nil {
		return nil, err
	}

	R := (&edwards25519.Point{}).ScalarBaseMult(r)

	kh := sha512.New()
	kh.Write(R.Bytes())
	kh.Write(A.Bytes())
	kh.Write(authData)
	hramDigest := kh.Sum(nil)
	k, err := edwards25519.NewScalar().SetUniformBytes(hramDigest)
	if err != nil {
		return nil, err
	}

	S := k.MultiplyAdd(k, s, r)

	return append(R.Bytes(), S.Bytes()...), nil
}

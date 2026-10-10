// Copyright 2026 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

// Package zstd enables automatic Zstandard compression for MySQL connections
// with compression enabled. Import it for its side effects:
//
//	import _ "github.com/go-sql-driver/mysql/zstd"
//
// This also registers the mysql database/sql driver. Connections use zstd when
// the server supports it and fall back to zlib otherwise. Compression remains
// disabled unless enabled through the DSN or mysql.EnableCompression.
package zstd

import (
	"sync"

	_ "github.com/go-sql-driver/mysql"
	"github.com/go-sql-driver/mysql/internal/compression"
	"github.com/klauspost/compress/zstd"
)

// Protocol packets have a three-byte uncompressed length. Bound both decoded
// output and the frame window; codec defaults allow much larger allocations.
const maxPacketSize = 1<<24 - 1

var encoderPool = sync.Pool{New: func() any {
	encoder, err := zstd.NewWriter(nil,
		zstd.WithEncoderConcurrency(1),
		// Fixed level 3 balances compression ratio and speed, favoring speed.
		// Importing this package alone does not enable compression.
		zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(compression.ZstdLevel)),
	)
	if err != nil {
		panic(err) // All options are fixed and valid.
	}
	return encoder
}}

var decoderPool = sync.Pool{New: func() any {
	decoder, err := zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderMaxMemory(maxPacketSize),
		zstd.WithDecoderMaxWindow(1<<24),
		zstd.WithDecodeAllCapLimit(true),
	)
	if err != nil {
		panic(err) // All options are fixed and valid.
	}
	return decoder
}}

type codec struct{}

func (codec) Encode(src, dst []byte) []byte {
	encoder := encoderPool.Get().(*zstd.Encoder)
	dst = encoder.EncodeAll(src, dst)
	encoderPool.Put(encoder)
	return dst
}

func (codec) Decode(src, dst []byte) ([]byte, error) {
	decoder := decoderPool.Get().(*zstd.Decoder)
	dst, err := decoder.DecodeAll(src, dst)
	decoderPool.Put(decoder)
	return dst, err
}

func init() {
	compression.Zstd = codec{}
}

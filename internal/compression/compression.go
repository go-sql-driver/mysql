// Copyright 2026 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

// Package compression connects optional compression implementations to the driver.
package compression

// Codec compresses and decompresses independent protocol packets. Both methods
// append to dst and must be safe for concurrent use. Decode must limit its output
// to cap(dst)-len(dst) bytes.
type Codec interface {
	Encode(src, dst []byte) []byte
	Decode(src, dst []byte) ([]byte, error)
}

// Zstd is set by mysql/zstd during package initialization. It remains nil when
// that package is not imported. It must not be changed after initialization.
var Zstd Codec

// ZstdLevel is the fixed compression level sent in HandshakeResponse and used
// by the encoder. Level 3 balances compression ratio and speed, favoring speed
// over the higher-compression modes.
const ZstdLevel = 3

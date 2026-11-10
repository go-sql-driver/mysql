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
	"testing"
)

func TestCompressedPacketDiagnosticBounds(t *testing.T) {
	for _, tc := range []struct {
		name                                 string
		buffered, compressed, decoded, limit int
		want                                 error
	}{
		{"compressed input", 0, 65, 1, 64, ErrPktTooLarge},
		{"decoded output", 60, 1, 5, 64, ErrPktTooLarge},
		{"uncompressed output", 60, 5, 0, 64, ErrPktTooLarge},
		{"uncompressed at limit", 60, 4, 0, 64, nil},
		{"unlimited normal path", 60, 5, 0, 0, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn, mc := newRWMockConn(1)
			conn.data = make([]byte, 7)
			putUint24(conn.data[:3], tc.compressed)
			putUint24(conn.data[4:], tc.decoded)
			if tc.want == nil {
				conn.data = append(conn.data, bytes.Repeat([]byte{'x'}, tc.compressed)...)
			}
			c := newCompIO(mc)
			c.buff.Write(bytes.Repeat([]byte{'a'}, tc.buffered))
			err := c.readCompressedPacket(tc.limit)
			if err != tc.want {
				t.Fatalf("readCompressedPacket = %v, want %v", err, tc.want)
			}
			wantLen := tc.buffered
			if tc.want == nil {
				wantLen += tc.compressed
			}
			if c.buff.Len() != wantLen {
				t.Fatalf("decoded buffer = %d, want %d", c.buff.Len(), wantLen)
			}
		})
	}
}

func TestCompressedPacketDiagnosticZlibBounds(t *testing.T) {
	const limit, buffered = 64, 60
	const remaining = limit - buffered
	for _, tc := range []struct {
		name   string
		actual int
		want   error
	}{
		{"above remaining limit", 32, ErrPktTooLarge},
		{"exactly remaining limit", remaining, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var compressed bytes.Buffer
			if err := zCompress(bytes.Repeat([]byte{'x'}, tc.actual), &compressed); err != nil {
				t.Fatal(err)
			}
			if compressed.Len() > limit {
				t.Fatal("test payload would fail the compressed-input header guard")
			}
			conn, mc := newRWMockConn(1)
			conn.data = make([]byte, 7)
			putUint24(conn.data[:3], compressed.Len())
			// Both headers pass the decoded-output guard. The oversized
			// case deliberately understates the real zlib output length.
			putUint24(conn.data[4:], remaining)
			conn.data = append(conn.data, compressed.Bytes()...)
			c := newCompIO(mc)
			prefix := bytes.Repeat([]byte{'a'}, buffered)
			c.buff.Write(prefix)
			if err := c.readCompressedPacket(limit); err != tc.want {
				t.Fatalf("readCompressedPacket = %v, want %v", err, tc.want)
			}
			// The overflow sentinel is the only byte allowed beyond the
			// remaining limit. This also proves zDecompress was reached,
			// rather than returning ErrPktTooLarge from a header guard.
			want := append(prefix, bytes.Repeat([]byte{'x'}, min(tc.actual, remaining+1))...)
			if !bytes.Equal(c.buff.Bytes(), want) {
				t.Fatalf("decoded buffer = %q (%d bytes), want %q (%d bytes)", c.buff.Bytes(), c.buff.Len(), want, len(want))
			}
		})
	}
}

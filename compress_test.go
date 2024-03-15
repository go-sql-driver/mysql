// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2024 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"testing"
)

func makeRandByteSlice(size int) []byte {
	randBytes := make([]byte, size)
	rand.Read(randBytes)
	return randBytes
}

func newMockConn() *mysqlConn {
	newConn := &mysqlConn{cfg: NewConfig()}
	return newConn
}

func newMockBuf(data []byte) buffer {
	return buffer{
		buf:    data,
		length: len(data),
	}
}

// compressHelper compresses uncompressedPacket and checks state variables
func compressHelper(t *testing.T, mc *mysqlConn, uncompressedPacket []byte) []byte {
	// get status variables

	cs := mc.compressSequence

	var b bytes.Buffer
	cw := newCompressor(mc, &b)

	n, err := cw.Write(uncompressedPacket)

	if err != nil {
		t.Fatal(err.Error())
	}

	if n != len(uncompressedPacket) {
		t.Fatalf("expected to write %d bytes, wrote %d bytes", len(uncompressedPacket), n)
	}

	if len(uncompressedPacket) > 0 {
		if mc.compressSequence != (cs + 1) {
			t.Fatalf("mc.compressionSequence updated incorrectly, expected %d and saw %d", (cs + 1), mc.compressSequence)
		}

	} else {
		if mc.compressSequence != cs {
			t.Fatalf("mc.compressionSequence updated incorrectly for case of empty write, expected %d and saw %d", cs, mc.compressSequence)
		}
	}

	return b.Bytes()
}

// uncompressHelper uncompresses compressedPacket and checks state variables
func uncompressHelper(t *testing.T, mc *mysqlConn, compressedPacket []byte, expSize int) []byte {
	// get status variables
	cs := mc.compressSequence

	// mocking out buf variable
	mc.buf = newMockBuf(compressedPacket)
	cr := newCompressor(mc, nil)

	uncompressedPacket, err := cr.readNext(expSize)
	if err != nil {
		if err != io.EOF {
			t.Fatalf("non-nil/non-EOF error when reading contents: %s", err.Error())
		}
	}

	if expSize > 0 {
		if mc.compressSequence != (cs + 1) {
			t.Fatalf("mc.compressionSequence updated incorrectly, expected %d and saw %d", (cs + 1), mc.compressSequence)
		}
	} else {
		if mc.compressSequence != cs {
			t.Fatalf("mc.compressionSequence updated incorrectly for case of empty read, expected %d and saw %d", cs, mc.compressSequence)
		}
	}
	return uncompressedPacket
}

// roundtripHelper compresses then uncompresses uncompressedPacket and checks state variables
func roundtripHelper(t *testing.T, cSend *mysqlConn, cReceive *mysqlConn, uncompressedPacket []byte) []byte {
	compressed := compressHelper(t, cSend, uncompressedPacket)
	return uncompressHelper(t, cReceive, compressed, len(uncompressedPacket))
}

// TestRoundtrip tests two connections, where one is reading and the other is writing
func TestRoundtrip(t *testing.T) {
	tests := []struct {
		uncompressed []byte
		desc         string
	}{
		{uncompressed: []byte("a"),
			desc: "a"},
		{uncompressed: []byte{0},
			desc: "0 byte"},
		{uncompressed: []byte("hello world"),
			desc: "hello world"},
		{uncompressed: make([]byte, 100),
			desc: "100 bytes"},
		{uncompressed: make([]byte, 32768),
			desc: "32768 bytes"},
		{uncompressed: make([]byte, 330000),
			desc: "33000 bytes"},
		{uncompressed: make([]byte, 0),
			desc: "nothing"},
		{uncompressed: makeRandByteSlice(10),
			desc: "10 rand bytes",
		},
		{uncompressed: makeRandByteSlice(100),
			desc: "100 rand bytes",
		},
		{uncompressed: makeRandByteSlice(32768),
			desc: "32768 rand bytes",
		},
		{uncompressed: makeRandByteSlice(33000),
			desc: "33000 rand bytes",
		},
	}

	cSend := newMockConn()
	cReceive := newMockConn()

	for _, test := range tests {
		s := fmt.Sprintf("Test roundtrip with %s", test.desc)

		uncompressed := roundtripHelper(t, cSend, cReceive, test.uncompressed)
		if !bytes.Equal(uncompressed, test.uncompressed) {
			t.Fatalf("%s: roundtrip failed", s)
		}
	}
}

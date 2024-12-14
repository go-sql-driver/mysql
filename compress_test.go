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
	"io"
	"testing"
)

func makeRandByteSlice(size int) []byte {
	randBytes := make([]byte, size)
	rand.Read(randBytes)
	return randBytes
}

// compressHelper compresses uncompressedPacket and checks state variables
func compressHelper(t *testing.T, mc *mysqlConn, uncompressedPacket []byte) []byte {
	conn := new(mockConn)
	mc.netConn = conn

	err := mc.writePacket(append(make([]byte, 4), uncompressedPacket...))
	if err != nil {
		t.Fatal(err)
	}

	return conn.written
}

// uncompressHelper uncompresses compressedPacket and checks state variables
func uncompressHelper(t *testing.T, mc *mysqlConn, compressedPacket []byte) []byte {
	// mocking out buf variable
	conn := new(mockConn)
	conn.data = compressedPacket
	mc.netConn = conn

	uncompressedPacket, err := mc.readPacket()
	if err != nil {
		if err != io.EOF {
			t.Fatalf("non-nil/non-EOF error when reading contents: %s", err.Error())
		}
	}
	return uncompressedPacket
}

// roundtripHelper compresses then uncompresses uncompressedPacket and checks state variables
func roundtripHelper(t *testing.T, cSend *mysqlConn, cReceive *mysqlConn, uncompressedPacket []byte) []byte {
	compressed := compressHelper(t, cSend, uncompressedPacket)
	return uncompressHelper(t, cReceive, compressed)
}

// TestRoundtrip tests two connections, where one is reading and the other is writing
func TestRoundtrip(t *testing.T) {
	tests := []struct {
		uncompressed []byte
		desc         string
	}{
		{uncompressed: []byte("a"),
			desc: "a"},
		{uncompressed: []byte("hello world"),
			desc: "hello world"},
		{uncompressed: make([]byte, 100),
			desc: "100 bytes"},
		{uncompressed: make([]byte, 32768),
			desc: "32768 bytes"},
		{uncompressed: make([]byte, 330000),
			desc: "33000 bytes"},
		{uncompressed: makeRandByteSlice(10),
			desc: "10 rand bytes",
		},
		{uncompressed: makeRandByteSlice(100),
			desc: "100 rand bytes",
		},
		{uncompressed: makeRandByteSlice(32768),
			desc: "32768 rand bytes",
		},
		{uncompressed: bytes.Repeat(makeRandByteSlice(100), 10000),
			desc: "100 rand * 10000 repeat bytes",
		},
	}

	_, cSend := newRWMockConn(0)
	cSend.compress = true
	cSend.compIO = newCompIO(cSend)
	_, cReceive := newRWMockConn(0)
	cReceive.compress = true
	cReceive.compIO = newCompIO(cReceive)

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			cSend.resetSequence()
			cReceive.resetSequence()

			uncompressed := roundtripHelper(t, cSend, cReceive, test.uncompressed)
			if len(uncompressed) != len(test.uncompressed) {
				t.Errorf("uncompressed size is unexpected. expected %d but got %d",
					len(test.uncompressed), len(uncompressed))
			}
			if !bytes.Equal(uncompressed, test.uncompressed) {
				t.Errorf("roundtrip failed")
			}
			if cSend.sequence != cReceive.sequence {
				t.Errorf("inconsistent sequence number: send=%v recv=%v",
					cSend.sequence, cReceive.sequence)
			}
			if cSend.compressSequence != cReceive.compressSequence {
				t.Errorf("inconsistent compress sequence number: send=%v recv=%v",
					cSend.compressSequence, cReceive.compressSequence)
			}
		})
	}
}

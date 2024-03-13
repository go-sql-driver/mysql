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

type mockBuf struct {
	reader io.Reader
}

func newMockBuf(reader io.Reader) *mockBuf {
	return &mockBuf{
		reader: reader,
	}
}

func (mb *mockBuf) readNext(need int) ([]byte, error) {
	data := make([]byte, need)
	_, err := mb.reader.Read(data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// compressHelper compresses uncompressedPacket and checks state variables
func compressHelper(t *testing.T, mc *mysqlConn, uncompressedPacket []byte) []byte {
	// get status variables

	cs := mc.compressionSequence

	var b bytes.Buffer
	connWriter := &b

	cw := newCompressedWriter(connWriter, mc)

	n, err := cw.Write(uncompressedPacket)

	if err != nil {
		t.Fatal(err.Error())
	}

	if n != len(uncompressedPacket) {
		t.Fatalf("expected to write %d bytes, wrote %d bytes", len(uncompressedPacket), n)
	}

	if len(uncompressedPacket) > 0 {
		if mc.compressionSequence != (cs + 1) {
			t.Fatalf("mc.compressionSequence updated incorrectly, expected %d and saw %d", (cs + 1), mc.compressionSequence)
		}

	} else {
		if mc.compressionSequence != cs {
			t.Fatalf("mc.compressionSequence updated incorrectly for case of empty write, expected %d and saw %d", cs, mc.compressionSequence)
		}
	}

	return b.Bytes()
}

// uncompressHelper uncompresses compressedPacket and checks state variables
func uncompressHelper(t *testing.T, mc *mysqlConn, compressedPacket []byte, expSize int) []byte {
	// get status variables
	cs := mc.compressionSequence

	// mocking out buf variable
	mockConnReader := bytes.NewReader(compressedPacket)
	mockBuf := newMockBuf(mockConnReader)

	cr := newCompressedReader(mockBuf, mc)

	uncompressedPacket, err := cr.readNext(expSize)
	if err != nil {
		if err != io.EOF {
			t.Fatalf("non-nil/non-EOF error when reading contents: %s", err.Error())
		}
	}

	if expSize > 0 {
		if mc.compressionSequence != (cs + 1) {
			t.Fatalf("mc.compressionSequence updated incorrectly, expected %d and saw %d", (cs + 1), mc.compressionSequence)
		}
	} else {
		if mc.compressionSequence != cs {
			t.Fatalf("mc.compressionSequence updated incorrectly for case of empty read, expected %d and saw %d", cs, mc.compressionSequence)
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

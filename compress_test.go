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
	newConn := &mysqlConn{}
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

	cw := NewCompressedWriter(connWriter, mc)

	n, err := cw.Write(uncompressedPacket)

	if err != nil {
		t.Fatal(err.Error())
	}

	if n != len(uncompressedPacket) {
		t.Fatal(fmt.Sprintf("expected to write %d bytes, wrote %d bytes", len(uncompressedPacket), n))
	}

	if len(uncompressedPacket) > 0 {

		if mc.compressionSequence != (cs + 1) {
			t.Fatal(fmt.Sprintf("mc.compressionSequence updated incorrectly, expected %d and saw %d", (cs + 1), mc.compressionSequence))
		}

	} else {
		if mc.compressionSequence != cs {
			t.Fatal(fmt.Sprintf("mc.compressionSequence updated incorrectly for case of empty write, expected %d and saw %d", cs, mc.compressionSequence))
		}
	}

	return b.Bytes()
}

// roundtripHelper compresses then uncompresses uncompressedPacket and checks state variables
func roundtripHelper(t *testing.T, cSend *mysqlConn, cReceive *mysqlConn, uncompressedPacket []byte) []byte {
	compressed := compressHelper(t, cSend, uncompressedPacket)
	return uncompressHelper(t, cReceive, compressed, len(uncompressedPacket))
}

// uncompressHelper uncompresses compressedPacket and checks state variables
func uncompressHelper(t *testing.T, mc *mysqlConn, compressedPacket []byte, expSize int) []byte {
	// get status variables
	cs := mc.compressionSequence

	// mocking out buf variable
	mockConnReader := bytes.NewReader(compressedPacket)
	mockBuf := newMockBuf(mockConnReader)

	cr := NewCompressedReader(mockBuf, mc)

	uncompressedPacket, err := cr.readNext(expSize)
	if err != nil {
		if err != io.EOF {
			t.Fatal(fmt.Sprintf("non-nil/non-EOF error when reading contents: %s", err.Error()))
		}
	}

	if expSize > 0 {
		if mc.compressionSequence != (cs + 1) {
			t.Fatal(fmt.Sprintf("mc.compressionSequence updated incorrectly, expected %d and saw %d", (cs + 1), mc.compressionSequence))
		}
	} else {
		if mc.compressionSequence != cs {
			t.Fatal(fmt.Sprintf("mc.compressionSequence updated incorrectly for case of empty read, expected %d and saw %d", cs, mc.compressionSequence))
		}
	}
	return uncompressedPacket
}

// TestCompressedReaderThenWriter tests reader and writer seperately.
func TestCompressedReaderThenWriter(t *testing.T) {

	makeTestUncompressedPacket := func(size int) []byte {
		uncompressedHeader := make([]byte, 4)
		uncompressedHeader[0] = byte(size)
		uncompressedHeader[1] = byte(size >> 8)
		uncompressedHeader[2] = byte(size >> 16)

		payload := make([]byte, size)
		for i := range payload {
			payload[i] = 'b'
		}

		uncompressedPacket := append(uncompressedHeader, payload...)
		return uncompressedPacket
	}

	tests := []struct {
		compressed   []byte
		uncompressed []byte
		desc         string
	}{
		{compressed: []byte{5, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 'a'},
			uncompressed: []byte{1, 0, 0, 0, 'a'},
			desc:         "a"},
		{compressed: []byte{10, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 'g', 'o', 'l', 'a', 'n', 'g'},
			uncompressed: []byte{6, 0, 0, 0, 'g', 'o', 'l', 'a', 'n', 'g'},
			desc:         "golang"},
		{compressed: []byte{19, 0, 0, 0, 104, 0, 0, 120, 156, 74, 97, 96, 96, 72, 162, 3, 0, 4, 0, 0, 255, 255, 182, 165, 38, 173},
			uncompressed: makeTestUncompressedPacket(100),
			desc:         "100 bytes letter b"},
		{compressed: []byte{63, 0, 0, 0, 236, 128, 0, 120, 156, 236, 192, 129, 0, 0, 0, 8, 3, 176, 179, 70, 18, 110, 24, 129, 124, 187, 77, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 168, 241, 1, 0, 0, 255, 255, 42, 107, 93, 24},
			uncompressed: makeTestUncompressedPacket(33000),
			desc:         "33000 bytes letter b"},
	}

	for _, test := range tests {
		s := fmt.Sprintf("Test compress uncompress with %s", test.desc)

		// test uncompression only
		c := newMockConn()
		uncompressed := uncompressHelper(t, c, test.compressed, len(test.uncompressed))
		if bytes.Compare(uncompressed, test.uncompressed) != 0 {
			t.Fatal(fmt.Sprintf("%s: uncompression failed", s))
		}

		// test compression only
		c = newMockConn()
		compressed := compressHelper(t, c, test.uncompressed)
		if bytes.Compare(compressed, test.compressed) != 0 {
			t.Fatal(fmt.Sprintf("%s: compression failed", s))
		}
	}
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
		//t.Run(s, func(t *testing.T) {

		uncompressed := roundtripHelper(t, cSend, cReceive, test.uncompressed)
		if bytes.Compare(uncompressed, test.uncompressed) != 0 {
			t.Fatal(fmt.Sprintf("%s: roundtrip failed", s))
		}

		//})
	}
}

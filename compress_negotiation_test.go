package mysql

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/go-sql-driver/mysql/internal/compression"
)

// Only negotiation uses this codec. Real zstd exchanges are tested in mysql/zstd
// so the driver's own test binary also builds without klauspost/compress.
type negotiationCodec struct{}

func (negotiationCodec) Encode(src, dst []byte) []byte { return append(dst, src...) }
func (negotiationCodec) Decode(src, dst []byte) ([]byte, error) {
	return append(dst, src...), nil
}

func TestCompressionNegotiation(t *testing.T) {
	for _, tc := range []struct {
		name       string
		imported   bool
		enabled    bool
		server     capabilityFlag
		negotiated capabilityFlag
	}{
		{name: "disabled", imported: true, server: clientCompress | clientZstdCompression},
		{name: "zlib without import", enabled: true, server: clientCompress | clientZstdCompression, negotiated: clientCompress},
		{name: "zstd unavailable without import", enabled: true, server: clientZstdCompression},
		{name: "prefer zstd", imported: true, enabled: true, server: clientCompress | clientZstdCompression, negotiated: clientZstdCompression},
		{name: "zstd only", imported: true, enabled: true, server: clientZstdCompression, negotiated: clientZstdCompression},
		{name: "fallback zlib", imported: true, enabled: true, server: clientCompress, negotiated: clientCompress},
		{name: "no server compression", imported: true, enabled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			previous := compression.Zstd
			t.Cleanup(func() { compression.Zstd = previous })
			compression.Zstd = nil
			if tc.imported {
				compression.Zstd = negotiationCodec{}
			}
			conn, mc := newRWMockConn(1)
			mc.cfg.compress = tc.enabled
			mc.cfg.User = "user"
			mc.cfg.encodedAttributes = "attributes"
			mc.initCapabilities(clientMySQL|clientProtocol41|clientConnectAttrs|tc.server, 0)
			got := mc.capabilities & (clientCompress | clientZstdCompression)
			if got != tc.negotiated {
				t.Fatalf("negotiated = %x, want %x", got, tc.negotiated)
			}
			if err := mc.writeHandshakeResponsePacket([]byte("response"), defaultAuthPlugin); err != nil {
				t.Fatal(err)
			}
			packet := conn.written[4:]
			if capabilityFlag(binary.LittleEndian.Uint32(packet)) != mc.capabilities {
				t.Fatal("HandshakeResponse has incorrect capabilities")
			}
			wantSuffix := appendLengthEncodedString(nil, "attributes")
			if tc.negotiated == clientZstdCompression {
				wantSuffix = append(wantSuffix, compression.ZstdLevel)
			}
			if !bytes.HasSuffix(packet, wantSuffix) {
				t.Fatalf("HandshakeResponse tail = %x, want %x", packet[len(packet)-len(wantSuffix):], wantSuffix)
			}
			c := newCompIO(mc)
			if (c.zstd != nil) != (tc.negotiated == clientZstdCompression) {
				t.Fatal("codec does not match negotiated compression")
			}
		})
	}
}

func TestZstdSSLRequest(t *testing.T) {
	previous := compression.Zstd
	t.Cleanup(func() { compression.Zstd = previous })
	compression.Zstd = negotiationCodec{}
	conn, mc := newRWMockConn(1)
	mc.cfg.compress = true
	// initCapabilities takes TLS configuration into account. Use a preselected
	// capability set here to inspect the SSLRequest wire layout directly.
	mc.capabilities = clientMySQL | clientProtocol41 | clientSSL | clientZstdCompression
	if err := mc.writeSSLRequestPacket(); err != nil {
		t.Fatal(err)
	}
	sslRequest := bytes.Clone(conn.written[4:])
	if len(sslRequest) != 32 {
		t.Fatalf("SSLRequest size = %d, want 32", len(sslRequest))
	}
	conn.written = nil
	if err := mc.writeHandshakeResponsePacket(nil, defaultAuthPlugin); err != nil {
		t.Fatal(err)
	}
	response := conn.written[4:]
	if !bytes.Equal(response[:32], sslRequest) || response[len(response)-1] != compression.ZstdLevel {
		t.Fatal("zstd level or shared TLS header is incorrect")
	}
}

type boundedCodec struct{ limit int }

func (boundedCodec) Encode(src, dst []byte) []byte { return append(dst, src...) }
func (c boundedCodec) Decode(src, dst []byte) ([]byte, error) {
	if cap(dst)-len(dst) != c.limit {
		return nil, fmt.Errorf("decoder output limit = %d, want %d", cap(dst)-len(dst), c.limit)
	}
	return append(dst, src...), nil
}

func TestCompressedPacketDecodeLimit(t *testing.T) {
	conn, mc := newRWMockConn(0)
	conn.data = []byte{1, 0, 0, 0, 1, 0, 0, 'x'}
	c := newCompIO(mc)
	c.zstd = boundedCodec{limit: 1}
	// A larger retained buffer must not relax the packet's decode limit.
	c.buff.Grow(4096)
	c.buff.WriteString("previous packet")
	if err := c.readCompressedPacket(); err != nil {
		t.Fatal(err)
	}
	if string(c.buff.Bytes()) != "previous packetx" {
		t.Fatal("decoding did not preserve pending bytes")
	}
}

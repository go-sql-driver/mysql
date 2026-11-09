// Copyright 2026 The Go-MySQL-Driver Authors. All rights reserved.
// Use of this source code is governed by the Mozilla Public License, v. 2.0.

package mysql

import (
	"bytes"
	"context"
	"crypto/tls"
	"database/sql/driver"
	"errors"
	"fmt"
	"log"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql/internal/compression"
	"github.com/klauspost/compress/zstd"
)

// Use a per-test implementation of the optional codec to avoid mutating the
// process-wide registration (and the import cycle through mysql/zstd).
type idleTestZstd struct {
	encoder *zstd.Encoder
	decoder *zstd.Decoder
}

var _ compression.Codec = (*idleTestZstd)(nil)

func (c *idleTestZstd) Encode(src, dst []byte) []byte          { return c.encoder.EncodeAll(src, dst) }
func (c *idleTestZstd) Decode(src, dst []byte) ([]byte, error) { return c.decoder.DecodeAll(src, dst) }

func idleTestConn(t *testing.T, version uint16, codec string) (*mysqlConn, net.Conn, net.Conn, *bytes.Buffer) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	client, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	serverRaw, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { serverRaw.Close() })
	server := serverRaw
	var transport net.Conn = client
	if version != 0 {
		certificate := authTestCertificate(t)
		serverTLS := tls.Server(serverRaw, &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: version, MaxVersion: version})
		clientTLS := tls.Client(client, &tls.Config{InsecureSkipVerify: true, MinVersion: version, MaxVersion: version})
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		handshake := make(chan error, 1)
		go func() { handshake <- serverTLS.HandshakeContext(ctx) }()
		if err := clientTLS.HandshakeContext(ctx); err != nil {
			t.Fatal(err)
		}
		if err := <-handshake; err != nil {
			t.Fatal(err)
		}
		transport, server = clientTLS, serverTLS
	}
	output := new(bytes.Buffer)
	cfg := NewConfig()
	cfg.Logger = log.New(output, "", 0)
	cfg.ReadTimeout = 10 * time.Second
	cfg.CheckConnLiveness = true
	mc := &mysqlConn{netConn: transport, rawConn: client, cfg: cfg, buf: newBuffer(), closech: make(chan struct{})}
	t.Cleanup(mc.close)
	if codec != "none" {
		mc.compress = true
		mc.compIO = newCompIO(mc)
		if codec == "zstd" {
			encoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
			if err != nil {
				t.Fatal(err)
			}
			decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecodeAllCapLimit(true), zstd.WithDecoderMaxMemory(maxPacketSize))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { encoder.Close(); decoder.Close() })
			mc.compIO.zstd = &idleTestZstd{encoder, decoder}
		}
	}
	return mc, server, serverRaw, output
}

func idleTestPacket(body []byte) []byte {
	header := make([]byte, 4)
	putUint24(header, len(body))
	header[3] = 77 // Diagnostics must ignore stale sequence numbers.
	return append(header, body...)
}

func idleTestError(number uint16, message string) []byte {
	return append([]byte{0xff, byte(number), byte(number >> 8), '#', 'H', 'Y', '0', '0', '0'}, message...)
}

func idleTestFrame(t *testing.T, mc *mysqlConn, packet []byte) []byte {
	t.Helper()
	if !mc.compress {
		return packet
	}
	var compressed []byte
	if mc.compIO.zstd != nil {
		compressed = mc.compIO.zstd.Encode(packet, nil)
	} else {
		var buf bytes.Buffer
		if err := zCompress(packet, &buf); err != nil {
			t.Fatal(err)
		}
		compressed = buf.Bytes()
	}
	header := make([]byte, 7)
	putUint24(header, len(compressed))
	header[3] = 91
	putUint24(header[4:], len(packet))
	return append(header, compressed...)
}

func idleTestWrite(t *testing.T, conn net.Conn, data []byte) {
	t.Helper()
	if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(data); err != nil {
		t.Fatal(err)
	}
}

func idleTestWaitReadable(t *testing.T, conn net.Conn) {
	t.Helper()
	switch runtime.GOOS {
	case "linux", "darwin", "dragonfly", "freebsd", "netbsd", "openbsd", "solaris", "illumos":
	default:
		t.Skip("connCheck is not supported on this platform")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := connCheck(conn)
		if err == errUnexpectedRead {
			return
		}
		if err != nil {
			t.Fatalf("connCheck before diagnostic = %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("server bytes did not become readable")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestIdleErrorTransport(t *testing.T) {
	for _, transport := range []struct {
		name    string
		version uint16
	}{{"plain", 0}, {"TLS12", tls.VersionTLS12}, {"TLS13", tls.VersionTLS13}} {
		for _, codec := range []string{"none", "zlib", "zstd"} {
			for _, fin := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/FIN=%t", transport.name, codec, fin), func(t *testing.T) {
					t.Parallel()
					mc, server, raw, output := idleTestConn(t, transport.version, codec)
					const message = "The client was disconnected by the server because of inactivity."
					idleTestWrite(t, server, idleTestFrame(t, mc, idleTestPacket(idleTestError(4031, message))))
					if fin {
						if err := raw.(*net.TCPConn).CloseWrite(); err != nil {
							t.Fatal(err)
						}
					}
					idleTestWaitReadable(t, mc.rawConn)
					if err := mc.ResetSession(t.Context()); err != driver.ErrBadConn {
						t.Fatalf("ResetSession = %v, want ErrBadConn", err)
					}
					for _, want := range []string{"closing bad idle connection:", "4031", "HY000", message} {
						if !strings.Contains(output.String(), want) {
							t.Errorf("log %q does not contain %q", output.String(), want)
						}
					}
					if !mc.closed.Load() {
						t.Error("diagnostic left connection open")
					}
				})
			}
		}
	}
}

func TestIdleErrorRejectReadOnly(t *testing.T) {
	for _, number := range []uint16{1290, 1792, 1836} {
		t.Run(fmt.Sprint(number), func(t *testing.T) {
			t.Parallel()
			mc, server, _, _ := idleTestConn(t, 0, "none")
			mc.cfg.RejectReadOnly = true
			idleTestWrite(t, server, idleTestPacket(idleTestError(number, "original server error")))
			err := mc.idleError()
			var serverErr *MySQLError
			if !errors.As(err, &serverErr) || serverErr.Number != number || serverErr.Message != "original server error" {
				t.Fatalf("idleError = %v, want original error %d", err, number)
			}
		})
	}
}

func TestIdleErrorMalformed(t *testing.T) {
	oversized := make([]byte, 4)
	putUint24(oversized, maxIdleErrorPacketSize+1)
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"truncated header", []byte{10, 0}},
		{"truncated body", []byte{10, 0, 0, 0, 0xff, 1, 0}},
		{"empty body", idleTestPacket(nil)},
		{"short ERR", idleTestPacket([]byte{0xff, 1})},
		{"truncated SQLSTATE", idleTestPacket([]byte{0xff, 1, 0, '#', 'H', 'Y'})},
		{"non ERR", idleTestPacket([]byte{0, 0, 0, 0})},
		{"oversized header", oversized},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mc, server, _, _ := idleTestConn(t, 0, "none")
			idleTestWrite(t, server, tc.data)
			start := time.Now()
			if err := mc.idleError(); err != errUnexpectedRead {
				t.Fatalf("idleError = %v", err)
			}
			if elapsed := time.Since(start); elapsed > 2*time.Second {
				t.Fatalf("diagnostic took %s despite bounded timeout", elapsed)
			}
			if !mc.closed.Load() {
				t.Error("diagnostic left connection open")
			}
		})
	}
}

func TestIdleErrorCompressedLimits(t *testing.T) {
	for _, codec := range []string{"zlib", "zstd"} {
		for _, name := range []string{"compressed size", "uncompressed size", "expansion", "truncated header", "truncated body"} {
			t.Run(codec+"/"+name, func(t *testing.T) {
				t.Parallel()
				mc, server, _, _ := idleTestConn(t, 0, codec)
				data := make([]byte, 7)
				switch name {
				case "compressed size":
					putUint24(data, maxIdleErrorPacketSize+5)
				case "uncompressed size":
					putUint24(data[4:], maxIdleErrorPacketSize+5)
				case "expansion":
					data = idleTestFrame(t, mc, bytes.Repeat([]byte{'x'}, maxIdleErrorPacketSize*2))
					putUint24(data[4:], 32) // Deliberately lie about decoded size.
				case "truncated header":
					data = []byte{1, 0, 0}
				case "truncated body":
					putUint24(data, 10)
				}
				idleTestWrite(t, server, data)
				start := time.Now()
				if err := mc.idleError(); err != errUnexpectedRead {
					t.Fatalf("idleError = %v", err)
				}
				if time.Since(start) > 2*time.Second {
					t.Fatal("compressed diagnostic exceeded timeout")
				}
				// zlib may retain the one-byte overflow sentinel, but not the expanded body.
				if mc.compIO.buff.Len() > maxIdleErrorPacketSize+5 {
					t.Fatalf("retained %d decompressed bytes", mc.compIO.buff.Len())
				}
			})
		}
	}
}

func TestIdleErrorTLSPartialRecord(t *testing.T) {
	for _, version := range []uint16{tls.VersionTLS12, tls.VersionTLS13} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			t.Parallel()
			mc, _, raw, _ := idleTestConn(t, version, "none")
			// Announce an application-data record but never send its body. The normal
			// packet reader resets deadlines to ReadTimeout, so the independent close
			// must still interrupt TLS's io.ReadFull.
			idleTestWrite(t, raw, []byte{23, 3, 3, 0, 100})
			idleTestWaitReadable(t, mc.rawConn)
			start := time.Now()
			if err := mc.ResetSession(t.Context()); err != driver.ErrBadConn {
				t.Fatalf("ResetSession = %v", err)
			}
			if elapsed := time.Since(start); elapsed > 2*time.Second {
				t.Fatalf("partial TLS record blocked for %s (ReadTimeout %s)", elapsed, mc.cfg.ReadTimeout)
			}
			if !mc.closed.Load() {
				t.Error("diagnostic left connection open")
			}
		})
	}
}

func TestDecodeErrorPacketDiagnostic(t *testing.T) {
	for _, body := range [][]byte{nil, {0xff}, {0xff, 1}, {0, 1, 0}, {0xff, 1, 0, '#'}, {0xff, 1, 0, '#', 'H', 'Y', '0', '0'}} {
		if _, err := decodeErrorPacket(body); err != ErrMalformPkt {
			t.Errorf("decodeErrorPacket(%x) error = %v", body, err)
		}
	}
	for _, body := range [][]byte{{0xff, 0xbf, 0x0f}, append([]byte{0xff, 0xbf, 0x0f}, "legacy message"...), idleTestError(4031, "modern message")} {
		got, err := decodeErrorPacket(body)
		if err != nil || got.Number != 4031 {
			t.Fatalf("decodeErrorPacket(%x) = %v, %v", body, got, err)
		}
	}
}

func TestIdleErrorCompressionFrames(t *testing.T) {
	for _, codec := range []string{"zlib", "zstd"} {
		for _, framing := range []string{"uncompressed", "split", "accumulated expansion"} {
			t.Run(codec+"/"+framing, func(t *testing.T) {
				t.Parallel()
				mc, server, _, _ := idleTestConn(t, 0, codec)
				packet := idleTestPacket(idleTestError(4031, "idle timeout"))
				var wire []byte
				switch framing {
				case "uncompressed":
					wire = make([]byte, 7)
					putUint24(wire, len(packet))
					wire = append(wire, packet...)
				case "split":
					// Split both the inner header and the ERR body across compression frames.
					for _, part := range [][]byte{packet[:2], packet[2:8], packet[8:]} {
						wire = append(wire, idleTestFrame(t, mc, part)...)
					}
				case "accumulated expansion":
					// Leave a substantial partial body buffered before decoding a frame that
					// lies about its output size. The bound must include those buffered bytes.
					partial := make([]byte, maxIdleErrorPacketSize/2)
					putUint24(partial, maxIdleErrorPacketSize)
					partial[4] = 0xff
					wire = idleTestFrame(t, mc, partial)
					expansion := idleTestFrame(t, mc, bytes.Repeat([]byte{'x'}, maxIdleErrorPacketSize))
					putUint24(expansion[4:], 32)
					wire = append(wire, expansion...)
				}
				idleTestWrite(t, server, wire)
				err := mc.idleError()
				if framing == "accumulated expansion" {
					if err != errUnexpectedRead {
						t.Fatalf("idleError = %v", err)
					}
					if n := mc.compIO.buff.Len(); n > maxIdleErrorPacketSize+5 {
						t.Fatalf("retained %d decompressed bytes across frames", n)
					}
				} else {
					var serverErr *MySQLError
					if !errors.As(err, &serverErr) || serverErr.Number != 4031 || serverErr.Message != "idle timeout" {
						t.Fatalf("idleError = %v", err)
					}
				}
			})
		}
	}
}

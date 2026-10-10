// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2026 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package zstd

import (
	"bytes"
	"compress/zlib"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

const (
	clientCompress = 1 << 5
	clientZstd     = 1 << 26
)

func packet(seq byte, data []byte) []byte {
	return append([]byte{byte(len(data)), byte(len(data) >> 8), byte(len(data) >> 16), seq}, data...)
}

func greeting(caps uint32) []byte {
	data := append([]byte{10}, "8.4.0\x00"...)
	data = binary.LittleEndian.AppendUint32(data, 1)
	data = append(data, "01234567\x00"...)
	data = binary.LittleEndian.AppendUint16(data, uint16(caps))
	data = append(data, 45, 2, 0)
	data = binary.LittleEndian.AppendUint16(data, uint16(caps>>16))
	data = append(data, 21)
	data = append(data, make([]byte, 10)...)
	data = append(data, "89abcdefghij\x00mysql_native_password\x00"...)
	return packet(0, data)
}

func readPacket(conn net.Conn) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		return nil, err
	}
	data := make([]byte, int(header[0])|int(header[1])<<8|int(header[2])<<16)
	_, err := io.ReadFull(conn, data)
	return data, err
}

func readCommand(conn net.Conn, algorithm uint32) ([]byte, bool, error) {
	if algorithm == 0 {
		data, err := readPacket(conn)
		return data, false, err
	}
	var header [7]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		return nil, false, err
	}
	data := make([]byte, int(header[0])|int(header[1])<<8|int(header[2])<<16)
	if _, err := io.ReadFull(conn, data); err != nil {
		return nil, false, err
	}
	size := int(header[4]) | int(header[5])<<8 | int(header[6])<<16
	if size != 0 {
		var err error
		if algorithm == clientZstd {
			data, err = (codec{}).Decode(data, make([]byte, 0, size))
		} else {
			var reader io.ReadCloser
			reader, err = zlib.NewReader(bytes.NewReader(data))
			if err == nil {
				data, err = io.ReadAll(reader)
				reader.Close()
			}
		}
		if err != nil {
			return nil, true, err
		}
	}
	if len(data) < 4 || len(data)-4 != int(data[0])|int(data[1])<<8|int(data[2])<<16 {
		return nil, size != 0, fmt.Errorf("invalid inner packet size")
	}
	return data[4:], size != 0, nil
}

func writeOK(conn net.Conn, algorithm uint32) error {
	data := packet(1, []byte{0, 0, 0, 2, 0, 0, 0})
	if algorithm != 0 {
		size := len(data)
		if algorithm == clientZstd {
			data = (codec{}).Encode(data, nil)
		} else {
			var dst bytes.Buffer
			writer := zlib.NewWriter(&dst)
			writer.Write(data)
			writer.Close()
			data = dst.Bytes()
		}
		data = append([]byte{byte(len(data)), byte(len(data) >> 8), byte(len(data) >> 16), 1, byte(size), 0, 0}, data...)
	}
	_, err := conn.Write(data)
	return err
}

func TestConnectionCompression(t *testing.T) {
	for _, tc := range []struct {
		name    string
		server  uint32
		enabled bool
		want    uint32
	}{
		{"zstd preferred", clientCompress | clientZstd, true, clientZstd},
		{"zstd only", clientZstd, true, clientZstd},
		{"zlib fallback", clientCompress, true, clientCompress},
		{"no server compression", 0, true, 0},
		{"disabled", clientCompress | clientZstd, false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			client, server := net.Pipe()
			defer client.Close()
			defer server.Close()
			deadline, _ := ctx.Deadline()
			server.SetDeadline(deadline)
			query := "DO '" + string(bytes.Repeat([]byte("mysql zstd protocol"), 200)) + "'"
			serverResult := make(chan error, 1)
			go func() {
				defer server.Close()
				serverResult <- func() error {
					caps := uint32(1 | 1<<9 | 1<<15 | 1<<19 | 1<<21)
					if _, err := server.Write(greeting(caps | tc.server)); err != nil {
						return err
					}
					response, err := readPacket(server)
					if err != nil {
						return err
					}
					got := binary.LittleEndian.Uint32(response) & (clientCompress | clientZstd)
					if got != tc.want {
						return fmt.Errorf("compression capabilities = %x, want %x", got, tc.want)
					}
					if got == clientZstd && response[len(response)-1] != 3 {
						return fmt.Errorf("missing fixed zstd level")
					}
					// Authentication completes in the ordinary packet protocol.
					if _, err := server.Write(packet(2, []byte{0, 0, 0, 2, 0, 0, 0})); err != nil {
						return err
					}
					command, compressed, err := readCommand(server, got)
					if err != nil {
						return err
					}
					if !bytes.Equal(command, append([]byte{3}, query...)) || (got != 0 && !compressed) {
						return fmt.Errorf("query packet did not use negotiated compression")
					}
					if err := writeOK(server, got); err != nil {
						return err
					}
					command, compressed, err = readCommand(server, got)
					if err != nil {
						return err
					}
					if !bytes.Equal(command, []byte{0x0e}) || compressed {
						return fmt.Errorf("small ping packet should remain uncompressed")
					}
					if err := writeOK(server, got); err != nil {
						return err
					}
					command, _, err = readCommand(server, got)
					if err == nil && !bytes.Equal(command, []byte{1}) {
						return fmt.Errorf("expected COM_QUIT")
					}
					return err
				}()
			}()
			cfg := mysql.NewConfig()
			cfg.User, cfg.Passwd = "user", "secret"
			cfg.DialFunc = func(context.Context, string, string) (net.Conn, error) { return client, nil }
			cfg.Apply(mysql.EnableCompression(tc.enabled))
			c, err := mysql.NewConnector(cfg)
			if err != nil {
				t.Fatal(err)
			}
			conn, err := c.Connect(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := conn.(driver.ExecerContext).ExecContext(ctx, query, nil); err != nil {
				t.Fatal(err)
			}
			if err := conn.(driver.Pinger).Ping(ctx); err != nil {
				t.Fatal(err)
			}
			if err := conn.Close(); err != nil {
				t.Fatal(err)
			}
			if err := <-serverResult; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestServerCompression(t *testing.T) {
	dsn := os.Getenv("MYSQL_ZSTD_TEST_DSN")
	if dsn == "" {
		t.Skip("set MYSQL_ZSTD_TEST_DSN to test a MySQL or MariaDB server")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	want := os.Getenv("MYSQL_ZSTD_TEST_ALGORITHM")
	if want == "" {
		var version string
		if err := conn.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(strings.ToLower(version), "mariadb") {
			want = "zstd"
		}
	}
	if want != "" {
		var name, algorithm string
		if err := conn.QueryRowContext(ctx, "SHOW SESSION STATUS LIKE 'Compression_algorithm'").Scan(&name, &algorithm); err != nil {
			t.Fatal(err)
		}
		if algorithm != want {
			t.Fatalf("server compression = %q, want %q", algorithm, want)
		}
		t.Logf("server compression = %s", algorithm)
	}
	for _, data := range [][]byte{[]byte("hello"), bytes.Repeat([]byte("mysql packet data"), 1100000)} {
		var got []byte
		if err := conn.QueryRowContext(ctx, "SELECT ?", data).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, data) {
			t.Fatalf("roundtrip size = %d, want %d", len(got), len(data))
		}
	}
	if err := conn.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
}

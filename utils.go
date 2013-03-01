// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2012 Julien Schmidt. All rights reserved.
// http://www.julienschmidt.com
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"io"
	"log"
	"math"
	"os"
	"regexp"
	"strconv"
	"strings"
)

// Logger
var (
	errLog *log.Logger
)

func init() {
	errLog = log.New(os.Stderr, "[MySQL] ", log.Ldate|log.Ltime|log.Lshortfile)

	dsnPattern = regexp.MustCompile(
		`^(?:(?P<user>.*?)(?::(?P<passwd>.*))?@)?` + // [user[:password]@]
			`(?:(?P<net>[^\(]*)(?:\((?P<addr>[^\)]*)\))?)?` + // [net[(addr)]]
			`\/(?P<dbname>.*?)` + // /dbname
			`(?:\?(?P<params>[^\?]*))?$`) // [?param1=value1&paramN=valueN]
}

// Data Source Name Parser
var dsnPattern *regexp.Regexp

func parseDSN(dsn string) *config {
	cfg := new(config)
	cfg.params = make(map[string]string)

	matches := dsnPattern.FindStringSubmatch(dsn)
	names := dsnPattern.SubexpNames()

	for i, match := range matches {
		switch names[i] {
		case "user":
			cfg.user = match
		case "passwd":
			cfg.passwd = match
		case "net":
			cfg.net = match
		case "addr":
			cfg.addr = match
		case "dbname":
			cfg.dbname = match
		case "params":
			for _, v := range strings.Split(match, "&") {
				param := strings.SplitN(v, "=", 2)
				if len(param) != 2 {
					continue
				}
				cfg.params[param[0]] = param[1]
			}
		}
	}

	// Set default network if empty
	if cfg.net == "" {
		cfg.net = "tcp"
	}

	// Set default adress if empty
	if cfg.addr == "" {
		cfg.addr = "127.0.0.1:3306"
	}

	return cfg
}

// Encrypt password using 4.1+ method
// http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#4.1_and_later
func scramblePassword(scramble, password []byte) (result []byte) {
	if len(password) == 0 {
		return
	}

	// stage1Hash = SHA1(password)
	crypt := sha1.New()
	crypt.Write(password)
	stage1Hash := crypt.Sum(nil)

	// scrambleHash = SHA1(scramble + SHA1(stage1Hash))
	// inner Hash
	crypt.Reset()
	crypt.Write(stage1Hash)
	scrambleHash := crypt.Sum(nil)

	// outer Hash
	crypt.Reset()
	crypt.Write(scramble)
	crypt.Write(scrambleHash)
	scrambleHash = crypt.Sum(nil)

	// token = scrambleHash XOR stage1Hash
	result = make([]byte, 20)
	for i := range result {
		result[i] = scrambleHash[i] ^ stage1Hash[i]
	}
	return
}

/******************************************************************************
*                       Read data-types from bytes                            *
******************************************************************************/

// Read a slice from the data slice
func readSlice(data []byte, delim byte) (slice []byte, err error) {
	pos := bytes.IndexByte(data, delim)
	if pos > -1 {
		slice = data[:pos]
	} else {
		slice = data
		err = io.EOF
	}
	return
}

func readLengthEnodedString(data []byte) ([]byte, bool, int, error) {
	// Get length
	num, isNull, n, err := bytesToLengthEncodedInteger(data)
	if err != nil || isNull {
		return nil, isNull, n, err
	}

	// Check data length
	if len(data) < n+int(num) {
		return nil, true, n, io.EOF
	}

	return data[n : n+int(num)], isNull, n + int(num), err
}

func readAndDropLengthEnodedString(data []byte) (n int, err error) {
	// Get length
	num, _, n, err := bytesToLengthEncodedInteger(data)
	if err != nil || num < 1 {
		return n, err
	}

	// Check data length
	if len(data) < n+int(num) {
		return n, io.EOF
	}

	return n + int(num), err
}

/******************************************************************************
*                       Convert from and to bytes                             *
******************************************************************************/

func uint24ToBytes(n uint32) (b []byte) {
	b = make([]byte, 3)
	for i := uint8(0); i < 3; i++ {
		b[i] = byte(n >> (i << 3))
	}
	return
}

func uint32ToBytes(n uint32) (b []byte) {
	b = make([]byte, 4)
	for i := uint8(0); i < 4; i++ {
		b[i] = byte(n >> (i << 3))
	}
	return
}

func uint64ToBytes(n uint64) (b []byte) {
	b = make([]byte, 8)
	for i := uint8(0); i < 8; i++ {
		b[i] = byte(n >> (i << 3))
	}
	return
}

func int64ToBytes(n int64) []byte {
	return uint64ToBytes(uint64(n))
}

func bytesToFloat32(b []byte) float32 {
	return math.Float32frombits(binary.LittleEndian.Uint32(b))
}

func bytesToFloat64(b []byte) float64 {
	return math.Float64frombits(binary.LittleEndian.Uint64(b))
}

func float64ToBytes(f float64) []byte {
	return uint64ToBytes(math.Float64bits(f))
}

func bytesToLengthEncodedInteger(b []byte) (num uint64, isNull bool, n int, err error) {
	switch b[0] {

	// 251: NULL
	case 0xfb:
		n = 1
		isNull = true
		return

	// 252: value of following 2
	case 0xfc:
		n = 3

	// 253: value of following 3
	case 0xfd:
		n = 4

	// 254: value of following 8
	case 0xfe:
		n = 9

	// 0-250: value of first byte
	default:
		num = uint64(b[0])
		n = 1
		return
	}

	switch n - 1 {
	case 2:
		num = uint64(b[0]) | uint64(b[1])<<8
		return
	case 3:
		num = uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16
		return
	default:
		num = uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 |
			uint64(b[3])<<24 | uint64(b[4])<<32 | uint64(b[5])<<40 |
			uint64(b[6])<<48 | uint64(b[7])<<54
	}
	return
}

func lengthEncodedIntegerToBytes(n uint64) (b []byte) {
	switch {
	case n <= 250:
		b = []byte{byte(n)}

	case n <= 0xffff:
		b = []byte{0xfc, byte(n), byte(n >> 8)}

	case n <= 0xffffff:
		b = []byte{0xfd, byte(n), byte(n >> 8), byte(n >> 16)}
	}
	return
}

func intToByteStr(i int64) (b []byte) {
	return strconv.AppendInt(b, i, 10)
}

func uintToByteStr(u uint64) (b []byte) {
	return strconv.AppendUint(b, u, 10)
}

func float32ToByteStr(f float32) (b []byte) {
	return strconv.AppendFloat(b, float64(f), 'f', -1, 32)
}

func float64ToByteStr(f float64) (b []byte) {
	return strconv.AppendFloat(b, f, 'f', -1, 64)
}

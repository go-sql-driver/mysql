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
	dbgLog *log.Logger
)

func init() {
	errLog = log.New(os.Stderr, "[MySQL] ", log.LstdFlags)
	dbgLog = log.New(os.Stdout, "[MySQL] ", log.LstdFlags)
	
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
func readSlice(data []byte, delim byte) (slice []byte, e error) {
	pos := bytes.IndexByte(data, delim)
	if pos > -1 {
		slice = data[:pos]
	} else {
		slice = data
		e = io.EOF
	}
	return
}

func readLengthCodedBinary(data []byte) (b []byte, n int, isNull bool, e error) {
	// Get length
	num, n, e := bytesToLengthCodedBinary(data)
	if e != nil {
		return
	}

	// Check data length
	if len(data) < n+int(num) {
		e = io.EOF
		return
	}

	// Check if null
	if data[0] == 251 {
		isNull = true
	} else {
		isNull = false
	}

	// Get bytes
	b = data[n : n+int(num)]
	n += int(num)
	return
}

func readAndDropLengthCodedBinary(data []byte) (n int, e error) {
	// Get length
	num, n, e := bytesToLengthCodedBinary(data)
	if e != nil {
		return
	}

	// Check data length
	if len(data) < n+int(num) {
		e = io.EOF
		return
	}

	n += int(num)
	return
}

/******************************************************************************
*                       Convert from and to bytes                             *
******************************************************************************/

func byteToUint8(b byte) (n uint8) {
	n |= uint8(b)
	return
}

func bytesToUint16(b []byte) (n uint16) {
	n |= uint16(b[0])
	n |= uint16(b[1]) << 8
	return
}

func uint24ToBytes(n uint32) (b []byte) {
	b = make([]byte, 3)
	for i := uint8(0); i < 3; i++ {
		b[i] = byte(n >> (i * 8))
	}
	return
}

func bytesToUint32(b []byte) (n uint32) {
	for i := uint8(0); i < 4; i++ {
		n |= uint32(b[i]) << (i * 8)
	}
	return
}

func uint32ToBytes(n uint32) (b []byte) {
	b = make([]byte, 4)
	for i := uint8(0); i < 4; i++ {
		b[i] = byte(n >> (i * 8))
	}
	return
}

func bytesToUint64(b []byte) (n uint64) {
	for i := uint8(0); i < 8; i++ {
		n |= uint64(b[i]) << (i * 8)
	}
	return
}

func uint64ToBytes(n uint64) (b []byte) {
	b = make([]byte, 8)
	for i := uint8(0); i < 8; i++ {
		b[i] = byte(n >> (i * 8))
	}
	return
}

func int64ToBytes(n int64) []byte {
	return uint64ToBytes(uint64(n))
}

func bytesToFloat32(b []byte) float32 {
	return math.Float32frombits(bytesToUint32(b))
}

func bytesToFloat64(b []byte) float64 {
	return math.Float64frombits(bytesToUint64(b))
}

func float64ToBytes(f float64) []byte {
	return uint64ToBytes(math.Float64bits(f))
}

func bytesToLengthCodedBinary(b []byte) (length uint64, n int, e error) {
	switch {

	// 0-250: value of first byte
	case b[0] <= 250:
		length = uint64(b[0])
		n = 1
		return

	// 251: NULL
	case b[0] == 251:
		length = 0
		n = 1
		return

	// 252: value of following 2
	case b[0] == 252:
		n = 3

	// 253: value of following 3
	case b[0] == 253:
		n = 4

	// 254: value of following 8
	case b[0] == 254:
		n = 9
	}

	if len(b) < n {
		e = io.EOF
		return
	}

	// get Length
	tmp := make([]byte, 8)
	copy(tmp, b[1:n])
	length = bytesToUint64(tmp)
	return
}

func lengthCodedBinaryToBytes(n uint64) (b []byte) {
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

func intToByteStr(i int64) (d *[]byte) {
	tmp := make([]byte, 0)
	tmp = strconv.AppendInt(tmp, i, 10)
	return &tmp
}

func uintToByteStr(u uint64) (d *[]byte) {
	tmp := make([]byte, 0)
	tmp = strconv.AppendUint(tmp, u, 10)
	return &tmp
}

func float32ToByteStr(f float32) (d *[]byte) {
	tmp := make([]byte, 0)
	tmp = strconv.AppendFloat(tmp, float64(f), 'f', -1, 32)
	return &tmp
}

func float64ToByteStr(f float64) (d *[]byte) {
	tmp := make([]byte, 0)
	tmp = strconv.AppendFloat(tmp, f, 'f', -1, 64)
	return &tmp
}

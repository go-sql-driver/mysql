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
	"crypto/sha1"
	"io"
	"log"
	"os"
	"regexp"
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
func scramblePassword(scramble, password []byte) []byte {
	if len(password) == 0 {
		return nil
	}

	// stage1Hash = SHA1(password)
	crypt := sha1.New()
	crypt.Write(password)
	stage1 := crypt.Sum(nil)

	// scrambleHash = SHA1(scramble + SHA1(stage1Hash))
	// inner Hash
	crypt.Reset()
	crypt.Write(stage1)
	hash := crypt.Sum(nil)

	// outer Hash
	crypt.Reset()
	crypt.Write(scramble)
	crypt.Write(hash)
	scramble = crypt.Sum(nil)

	// token = scrambleHash XOR stage1Hash
	for i := range scramble {
		scramble[i] ^= stage1[i]
	}
	return scramble
}

/******************************************************************************
*                       Convert from and to bytes                             *
******************************************************************************/

func uint64ToBytes(n uint64) []byte {
	return []byte{
		byte(n),
		byte(n >> 8),
		byte(n >> 16),
		byte(n >> 24),
		byte(n >> 32),
		byte(n >> 40),
		byte(n >> 48),
		byte(n >> 56),
	}
}

func uint64ToString(n uint64) []byte {
	var a [20]byte
	i := 20

	// U+0030 = 0
	// ...
	// U+0039 = 9

	var q uint64
	for n >= 10 {
		i--
		q = n / 10
		a[i] = uint8(n-q*10) + 0x30
		n = q
	}

	i--
	a[i] = uint8(n) + 0x30

	return a[i:]
}

// treats string value as unsigned integer representation
func stringToInt(b []byte) int {
	val := 0
	for i := range b {
		val *= 10
		val += int(b[i] - 0x30)
	}
	return val
}

func readLengthEnodedString(b []byte) ([]byte, bool, int, error) {
	// Get length
	num, isNull, n := readLengthEncodedInteger(b)
	if num < 1 {
		return nil, isNull, n, nil
	}

	n += int(num)

	// Check data length
	if len(b) >= n {
		return b[n-int(num) : n], false, n, nil
	}
	return nil, false, n, io.EOF
}

func skipLengthEnodedString(b []byte) (int, error) {
	// Get length
	num, _, n := readLengthEncodedInteger(b)
	if num < 1 {
		return n, nil
	}

	n += int(num)

	// Check data length
	if len(b) >= n {
		return n, nil
	}
	return n, io.EOF
}

func readLengthEncodedInteger(b []byte) (num uint64, isNull bool, n int) {
	switch b[0] {

	// 251: NULL
	case 0xfb:
		n = 1
		isNull = true
		return

	// 252: value of following 2
	case 0xfc:
		num = uint64(b[1]) | uint64(b[2])<<8
		n = 3
		return

	// 253: value of following 3
	case 0xfd:
		num = uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16
		n = 4
		return

	// 254: value of following 8
	case 0xfe:
		num = uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16 |
			uint64(b[4])<<24 | uint64(b[5])<<32 | uint64(b[6])<<40 |
			uint64(b[7])<<48 | uint64(b[8])<<54
		n = 9
		return
	}

	// 0-250: value of first byte
	num = uint64(b[0])
	n = 1
	return
}

func lengthEncodedIntegerToBytes(n uint64) []byte {
	switch {
	case n <= 250:
		return []byte{byte(n)}

	case n <= 0xffff:
		return []byte{0xfc, byte(n), byte(n >> 8)}

	case n <= 0xffffff:
		return []byte{0xfd, byte(n), byte(n >> 8), byte(n >> 16)}
	}
	return nil
}

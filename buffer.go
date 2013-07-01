// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2013 Julien Schmidt. All rights reserved.
// http://www.julienschmidt.com
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import "io"

const defaultBufSize = 4096

// A read buffer similar to bufio.Reader but zero-copy-ish
// Also highly optimized for this particular use case.
type buffer struct {
	buf    []byte
	rd     io.Reader
	idx    int
	length int
}

func newBuffer(rd io.Reader) *buffer {
	var b [defaultBufSize]byte
	return &buffer{
		buf: b[:],
		rd:  rd,
	}
}

// fill reads into the buffer until at least _need_ bytes are in it
func (b *buffer) fill(need int) (err error) {
	// move existing data to the beginning
	if b.length > 0 && b.idx > 0 {
		copy(b.buf[0:b.length], b.buf[b.idx:])
	}

	// grow buffer if necessary
	if need > len(b.buf) {
		newBuf := make([]byte, need)
		copy(newBuf, b.buf)
		b.buf = newBuf
	}

	b.idx = 0

	var n int
	for {
		n, err = b.rd.Read(b.buf[b.length:])
		b.length += n

		if b.length < need && err == nil {
			continue
		}
		return // err
	}
}

// returns next N bytes from buffer.
// The returned slice is only guaranteed to be valid until the next read
func (b *buffer) readNext(need int) (p []byte, err error) {
	if b.length < need {
		// refill
		err = b.fill(need) // err deferred
	}

	p = b.buf[b.idx : b.idx+need]
	b.idx += need
	b.length -= need
	return
}

// various allocation pools

var bytesPool = make(chan []byte, 16)

// may return unzeroed bytes
func getBytes(n int) []byte {
	select {
	case s := <-bytesPool:
		if cap(s) >= n {
			return s[:n]
		}
	default:
	}
	return make([]byte, n)
}

func putBytes(s []byte) {
	select {
	case bytesPool <- s:
	default:
	}
}

var fieldPool = make(chan []mysqlField, 16)

func getMysqlFields(n int) []mysqlField {
	select {
	case f := <-fieldPool:
		if cap(f) >= n {
			return f[:n]
		}
	default:
	}
	return make([]mysqlField, n)
}

func putMysqlFields(f []mysqlField) {
	select {
	case fieldPool <- f:
	default:
	}
}

var rowsPool = make(chan *mysqlRows, 16)

func getMysqlRows() *mysqlRows {
	select {
	case r := <-rowsPool:
		return r
	default:
	}
	return new(mysqlRows)
}

func putMysqlRows(r *mysqlRows) {
	*r = mysqlRows{} // zero it
	select {
	case rowsPool <- r:
	default:
	}
}

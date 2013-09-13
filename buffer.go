// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2013 The Go-MySQL-Driver Authors. All rights reserved.
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
	return
}

// returns next N bytes from buffer.
// The returned slice is only guaranteed to be valid until the next read
func (b *buffer) readNext(need int) (p []byte, err error) {
	if b.length < need {
		// refill
		err = b.fill(need) // err deferred
		if err == io.EOF && b.length >= need {
			err = nil
		}
	}

	p = b.buf[b.idx : b.idx+need]
	b.idx += need
	b.length -= need
	return
}

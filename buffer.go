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

type buffer struct {
	buf    []byte
	rd     io.Reader
	idx    int
	length int
}

func newBuffer(rd io.Reader) *buffer {
	return &buffer{
		buf: make([]byte, defaultBufSize),
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
		b.grow(need)
	}

	b.idx = 0

	var n int
	for b.length < need {
		n, err = b.rd.Read(b.buf[b.length:])
		b.length += n

		if err == nil {
			continue
		}
		return // err
	}

	return
}

// grow the buffer to at least the given size
// credit for this code snippet goes to Maxim Khitrov
// https://groups.google.com/forum/#!topic/golang-nuts/ETbw1ECDgRs
func (b *buffer) grow(size int) {
	// If append would be too expensive, alloc a new slice
	if size > 2*cap(b.buf) {
		newBuf := make([]byte, size)
		copy(newBuf, b.buf)
		b.buf = newBuf
		return
	}

	for cap(b.buf) < size {
		b.buf = append(b.buf[:cap(b.buf)], 0)
	}
	b.buf = b.buf[:cap(b.buf)]
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

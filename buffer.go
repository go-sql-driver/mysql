// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2013 Julien Schmidt. All rights reserved.
// http://www.julienschmidt.com
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"io"
)

const (
	defaultBufSize = 4096
)

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

// fill reads at least _need_ bytes in the buffer
// existing data in the buffer gets lost
func (b *buffer) fill(need int) (err error) {
	b.idx = 0
	b.length = 0

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

// returns next N bytes from buffer.
// The returned slice is only guaranteed to be valid until the next read
func (b *buffer) readNext(need int) (p []byte, err error) {
	// return slice from buffer if possible
	if b.length >= need {
		p = b.buf[b.idx : b.idx+need]
		b.idx += need
		b.length -= need
		return

	} else {
		p = make([]byte, need)
		has := 0

		// copy data that is already in the buffer
		if b.length > 0 {
			copy(p[0:b.length], b.buf[b.idx:])
			has = b.length
			need -= has
			b.idx = 0
			b.length = 0
		}

		// does the data fit into the buffer?
		if need < len(b.buf) {
			err = b.fill(need) // err deferred
			copy(p[has:has+need], b.buf[b.idx:])
			b.idx += need
			b.length -= need
			return

		} else {
			var n int
			for err == nil && need > 0 {
				n, err = b.rd.Read(p[has:])
				has += n
				need -= n
			}
		}
	}
	return
}

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

// read len(p) bytes
func (b *buffer) read(p []byte) (err error) {
	need := len(p)

	if b.length < need {
		if b.length > 0 {
			copy(p[0:b.length], b.buf[b.idx:])
			need -= b.length
			p = p[b.length:]

			b.idx = 0
			b.length = 0
		}

		if need >= len(b.buf) {
			var n int
			has := 0
			for err == nil && need > has {
				n, err = b.rd.Read(p[has:])
				has += n
			}
			return
		}

		err = b.fill(need) // err deferred
	}

	copy(p, b.buf[b.idx:])
	b.idx += need
	b.length -= need
	return
}

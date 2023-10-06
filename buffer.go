// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2013 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"io"
	"time"
)

const defaultBufSize = 4096
const maxCachedBufSize = 256 * 1024

// A buffer which is used for both reading and writing.
// This is possible since communication on each connection is synchronous.
// In other words, we can't write and read simultaneously on the same connection.
// The buffer is similar to bufio.Reader / Writer but zero-copy-ish
// Also highly optimized for this particular use case.
// This buffer is backed by two byte slices in a double-buffering scheme
type buffer struct {
	buf     []byte // buf is a byte buffer who's length and capacity are equal.
	mc      *mysqlConn
	idx     int
	length  int
	timeout time.Duration
	dbuf    [2][]byte // dbuf is an array with the two byte slices that back this buffer
	flipcnt uint      // flipccnt is the current buffer counter for double-buffering
}

// newBuffer allocates and returns a new buffer.
func newBuffer(mc *mysqlConn) buffer {
	fg := make([]byte, defaultBufSize)
	return buffer{
		buf:  fg,
		mc:   mc,
		dbuf: [2][]byte{fg, nil},
	}
}

// flip replaces the active buffer with the background buffer
// this is a delayed flip that simply increases the buffer counter;
// the actual flip will be performed the next time we call `buffer.fill`
func (b *buffer) flip() {
	b.flipcnt += 1
}

// fill reads into the buffer until at least _need_ bytes are in it
func (b *buffer) fill(need int) error {
	n := b.length
	// fill data into its double-buffering target: if we've called
	// flip on this buffer, we'll be copying to the background buffer,
	// and then filling it with network data; otherwise we'll just move
	// the contents of the current buffer to the front before filling it
	dest := b.dbuf[b.flipcnt&1]

	// grow buffer if necessary to fit the whole packet.
	if need > len(dest) {
		// Round up to the next multiple of the default size
		dest = make([]byte, ((need/defaultBufSize)+1)*defaultBufSize)

		// if the allocated buffer is not too large, move it to backing storage
		// to prevent extra allocations on applications that perform large reads
		if len(dest) <= maxCachedBufSize {
			b.dbuf[b.flipcnt&1] = dest
		}
	}

	// if we're filling the fg buffer, move the existing data to the start of it.
	// if we're filling the bg buffer, copy over the data
	if n > 0 {
		copy(dest[:n], b.buf[b.idx:])
	}

	b.buf = dest
	b.idx = 0

	for {
		var result readResult
		select {
		case result = <-b.mc.readRes:
		case <-b.mc.closech:
			return ErrInvalidConn
		}
		b.buf = append(b.buf[:n], result.data...)
		n += len(result.data)

		switch result.err {
		case nil:
			if n < need {
				continue
			}
			b.length = n
			return nil

		case io.EOF:
			if n >= need {
				b.length = n
				return nil
			}
			return io.ErrUnexpectedEOF

		default:
			return result.err
		}
	}
}

// returns next N bytes from buffer.
// The returned slice is only guaranteed to be valid until the next read
func (b *buffer) readNext(need int) ([]byte, error) {
	if b.length < need {
		// refill
		if err := b.fill(need); err != nil {
			return nil, err
		}
	}

	offset := b.idx
	b.idx += need
	b.length -= need
	return b.buf[offset:b.idx], nil
}

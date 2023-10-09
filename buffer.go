package mysql

import (
	"io"
	"net"
)

const defaultBufSize = 4096 // must be 2^n
// const maxCachedBufSize = 256 * 1024

type writeBuffer struct {
	buf []byte
}

// takeBuffer returns a buffer with the requested size.
// If possible, a slice from the existing buffer is returned.
// Otherwise a bigger buffer is made.
// Only one buffer (total) can be used at a time.
func (wb *writeBuffer) takeBuffer(length int) []byte {
	if length <= cap(wb.buf) {
		return wb.buf[:length]
	}
	if length <= defaultBufSize {
		wb.buf = make([]byte, length, defaultBufSize)
		return wb.buf
	}
	if length <= maxPacketSize {
		wb.buf = make([]byte, length)
		return wb.buf
	}
	return make([]byte, length)
}

func (wb *writeBuffer) store(buf []byte) {
	if cap(buf) < cap(wb.buf) || cap(buf) > maxPacketSize {
		return
	}
	wb.buf = buf
}

type readBuffer struct {
	buf []byte
	idx int
	nc  net.Conn
}

func newReadBuffer(nc net.Conn) readBuffer {
	return readBuffer{
		buf: make([]byte, 0, defaultBufSize),
		nc:  nc,
	}
}

// fill reads into the buffer until at least _need_ bytes are in it.
func (rb *readBuffer) fill(need int) error {
	var buf []byte
	if need <= cap(rb.buf) {
		buf = rb.buf[:0]
	} else {
		// Round up to the next multiple of the default size
		size := (need + defaultBufSize - 1) &^ (defaultBufSize - 1)
		buf = make([]byte, 0, size)
	}

	// move the existing data to the start of it.
	buf = append(buf, rb.buf[rb.idx:]...)
	rb.idx = 0

	for {
		n, err := rb.nc.Read(buf[len(buf):cap(buf)])
		buf = buf[:len(buf)+n]
		switch err {
		case nil:
			if len(buf) >= need {
				rb.buf = buf
				return nil
			}

		case io.EOF:
			if len(buf) >= need {
				rb.buf = buf
				return nil
			}
			return io.ErrUnexpectedEOF

		default:
			return err
		}
	}
}

// returns next N bytes from buffer.
// The returned slice is only guaranteed to be valid until the next read.
func (rb *readBuffer) readNext(need int) ([]byte, error) {
	if len(rb.buf)-rb.idx < need {
		if err := rb.fill(need); err != nil {
			return nil, err
		}
	}

	offset := rb.idx
	rb.idx += need
	return rb.buf[offset:rb.idx], nil
}

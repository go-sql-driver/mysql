package mysql

const defaultBufSize = 4096

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

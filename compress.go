package mysql

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"io"
	"os"
)

// for debugging wire protocol.
const debugTrace = false

type compressedReader struct {
	buf      packetReader
	bytesBuf []byte
	mc       *mysqlConn
	zr       io.ReadCloser
}

type compressedWriter struct {
	connWriter io.Writer
	mc         *mysqlConn
	zw         *zlib.Writer
}

func newCompressedReader(buf packetReader, mc *mysqlConn) *compressedReader {
	return &compressedReader{
		buf:      buf,
		bytesBuf: make([]byte, 0),
		mc:       mc,
	}
}

func newCompressedWriter(connWriter io.Writer, mc *mysqlConn) *compressedWriter {
	// level 1 or 2 is the best trade-off between speed and compression ratio
	zw, err := zlib.NewWriterLevel(new(bytes.Buffer), 2)
	if err != nil {
		panic(err) // compress/zlib return non-nil error only if level is invalid
	}
	return &compressedWriter{
		connWriter: connWriter,
		mc:         mc,
		zw:         zw,
	}
}

func (r *compressedReader) readNext(need int) ([]byte, error) {
	for len(r.bytesBuf) < need {
		if err := r.uncompressPacket(); err != nil {
			return nil, err
		}
	}

	data := r.bytesBuf[:need:need] // prevent caller writes into r.bytesBuf
	r.bytesBuf = r.bytesBuf[need:]
	return data, nil
}

func (r *compressedReader) uncompressPacket() error {
	header, err := r.buf.readNext(7) // size of compressed header
	if err != nil {
		return err
	}

	// compressed header structure
	comprLength := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
	uncompressedLength := int(uint32(header[4]) | uint32(header[5])<<8 | uint32(header[6])<<16)
	compressionSequence := uint8(header[3])
	if debugTrace {
		fmt.Fprintf(os.Stderr, "uncompress cmplen=%v uncomplen=%v seq=%v\n",
			comprLength, uncompressedLength, compressionSequence)
	}
	if compressionSequence != r.mc.compressionSequence {
		return ErrPktSync
	}
	r.mc.compressionSequence++

	comprData, err := r.buf.readNext(comprLength)
	if err != nil {
		return err
	}

	// if payload is uncompressed, its length will be specified as zero, and its
	// true length is contained in comprLength
	if uncompressedLength == 0 {
		r.bytesBuf = append(r.bytesBuf, comprData...)
		return nil
	}

	// write comprData to a bytes.buffer, then read it using zlib into data
	br := bytes.NewReader(comprData)
	if r.zr == nil {
		if r.zr, err = zlib.NewReader(br); err != nil {
			return err
		}
	} else {
		if err = r.zr.(zlib.Resetter).Reset(br, nil); err != nil {
			return err
		}
	}
	defer r.zr.Close()

	// use existing capacity in bytesBuf if possible
	offset := len(r.bytesBuf)
	if cap(r.bytesBuf)-offset < uncompressedLength {
		old := r.bytesBuf
		r.bytesBuf = make([]byte, offset, offset+uncompressedLength)
		copy(r.bytesBuf, old)
	}

	data := r.bytesBuf[offset : offset+uncompressedLength]
	lenRead := 0

	// http://grokbase.com/t/gg/golang-nuts/146y9ppn6b/go-nuts-stream-compression-with-compress-flate
	for lenRead < uncompressedLength {
		n, err := r.zr.Read(data[lenRead:])
		lenRead += n

		if err == io.EOF {
			if lenRead < uncompressedLength {
				return io.ErrUnexpectedEOF
			}
			break
		} else if err != nil {
			return err
		}
	}
	if lenRead != uncompressedLength {
		return fmt.Errorf("invalid compressed packet: uncompressed length in header is %d, actual %d",
			uncompressedLength, lenRead)
	}
	r.bytesBuf = r.bytesBuf[:offset+uncompressedLength]
	return nil
}

const maxPayloadLen = maxPacketSize - 4

var blankHeader = make([]byte, 7)

func (w *compressedWriter) Write(data []byte) (int, error) {
	totalBytes := len(data)
	dataLen := len(data)
	var buf bytes.Buffer

	for dataLen > 0 {
		payloadLen := dataLen
		if payloadLen > maxPayloadLen {
			payloadLen = maxPayloadLen
		}
		payload := data[:payloadLen]
		uncompressedLen := payloadLen

		if _, err := buf.Write(blankHeader); err != nil {
			return 0, err
		}

		// If payload is less than minCompressLength, don't compress.
		if uncompressedLen < minCompressLength {
			if _, err := buf.Write(payload); err != nil {
				return 0, err
			}
			uncompressedLen = 0
		} else {
			w.zw.Reset(&buf)
			if _, err := w.zw.Write(payload); err != nil {
				return 0, err
			}
			w.zw.Close()
		}

		if err := w.writeCompressedPacket(buf.Bytes(), uncompressedLen); err != nil {
			return 0, err
		}

		dataLen -= payloadLen
		data = data[payloadLen:]
		buf.Reset()
	}

	return totalBytes, nil
}

// writeCompressedPacket writes a compressed packet with header.
// data should start with 7 size space for header followed by payload.
func (w *compressedWriter) writeCompressedPacket(data []byte, uncompressedLen int) error {
	comprLength := len(data) - 7

	// compression header
	data[0] = byte(0xff & comprLength)
	data[1] = byte(0xff & (comprLength >> 8))
	data[2] = byte(0xff & (comprLength >> 16))

	data[3] = w.mc.compressionSequence

	// this value is never greater than maxPayloadLength
	data[4] = byte(0xff & uncompressedLen)
	data[5] = byte(0xff & (uncompressedLen >> 8))
	data[6] = byte(0xff & (uncompressedLen >> 16))

	if debugTrace {
		w.mc.cfg.Logger.Print(
			fmt.Sprintf(
				"writeCompressedPacket: comprLength=%v, uncompressedLen=%v, seq=%v",
				comprLength, uncompressedLen, int(data[3])))
	}

	if _, err := w.connWriter.Write(data); err != nil {
		w.mc.cfg.Logger.Print(err)
		return err
	}

	w.mc.compressionSequence++
	return nil
}

package mysql

import (
	"bytes"
	"compress/zlib"
	"io"
)

const (
	minCompressLength = 50
)

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
	return &compressedWriter{
		connWriter: connWriter,
		mc:         mc,
		zw:         zlib.NewWriter(new(bytes.Buffer)),
	}
}

func (r *compressedReader) readNext(need int) ([]byte, error) {
	for len(r.bytesBuf) < need {
		if err := r.uncompressPacket(); err != nil {
			return nil, err
		}
	}

	data := r.bytesBuf[:need]
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

	r.bytesBuf = append(r.bytesBuf, data...)

	return nil
}

func (w *compressedWriter) Write(data []byte) (int, error) {
	// when asked to write an empty packet, do nothing
	if len(data) == 0 {
		return 0, nil
	}

	totalBytes := len(data)
	length := len(data) - 4
	maxPayloadLength := maxPacketSize - 4
	blankHeader := make([]byte, 7)

	for length >= maxPayloadLength {
		payload := data[:maxPayloadLength]
		payloadLen := len(payload)

		bytesBuf := &bytes.Buffer{}
		bytesBuf.Write(blankHeader)
		w.zw.Reset(bytesBuf)
		if _, err := w.zw.Write(payload); err != nil {
			return 0, err
		}
		w.zw.Close()

		// if compression expands the payload, do not compress
		compressedPayload := bytesBuf.Bytes()
		if len(compressedPayload) > maxPayloadLength {
			compressedPayload = append(blankHeader, payload...)
			payloadLen = 0
		}

		if err := w.writeToNetwork(compressedPayload, payloadLen); err != nil {
			return 0, err
		}

		length -= maxPayloadLength
		data = data[maxPayloadLength:]
	}

	payloadLen := len(data)

	// do not attempt compression if packet is too small
	if payloadLen < minCompressLength {
		if err := w.writeToNetwork(append(blankHeader, data...), 0); err != nil {
			return 0, err
		}
		return totalBytes, nil
	}

	bytesBuf := &bytes.Buffer{}
	bytesBuf.Write(blankHeader)
	w.zw.Reset(bytesBuf)
	if _, err := w.zw.Write(data); err != nil {
		return 0, err
	}
	w.zw.Close()

	compressedPayload := bytesBuf.Bytes()

	if len(compressedPayload) > len(data) {
		compressedPayload = append(blankHeader, data...)
		payloadLen = 0
	}

	// add header and send over the wire
	if err := w.writeToNetwork(compressedPayload, payloadLen); err != nil {
		return 0, err
	}

	return totalBytes, nil

}

func (w *compressedWriter) writeToNetwork(data []byte, uncomprLength int) error {
	comprLength := len(data) - 7

	// compression header
	data[0] = byte(0xff & comprLength)
	data[1] = byte(0xff & (comprLength >> 8))
	data[2] = byte(0xff & (comprLength >> 16))

	data[3] = w.mc.compressionSequence

	// this value is never greater than maxPayloadLength
	data[4] = byte(0xff & uncomprLength)
	data[5] = byte(0xff & (uncomprLength >> 8))
	data[6] = byte(0xff & (uncomprLength >> 16))

	if _, err := w.connWriter.Write(data); err != nil {
		return err
	}

	w.mc.compressionSequence++
	return nil
}

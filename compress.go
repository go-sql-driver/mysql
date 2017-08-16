package mysql

import (
	"bytes"
	"compress/zlib"
	"io"
)

const (
	minCompressLength = 50
)

type packetReader interface {
	readNext(need int) ([]byte, error)
}

type compressedReader struct {
	buf      packetReader
	bytesBuf []byte
	mc       *mysqlConn
	zr       io.ReadCloser
}

type compressedWriter struct {
	connWriter io.Writer
	mc         *mysqlConn
	header     []byte
}

func NewCompressedReader(buf packetReader, mc *mysqlConn) *compressedReader {
	return &compressedReader{
		buf:      buf,
		bytesBuf: make([]byte, 0),
		mc:       mc,
	}
}

func NewCompressedWriter(connWriter io.Writer, mc *mysqlConn) *compressedWriter {
	return &compressedWriter{
		connWriter: connWriter,
		mc:         mc,
		header:     []byte{0, 0, 0, 0, 0, 0, 0},
	}
}

func (cr *compressedReader) readNext(need int) ([]byte, error) {
	for len(cr.bytesBuf) < need {
		err := cr.uncompressPacket()
		if err != nil {
			return nil, err
		}
	}

	data := cr.bytesBuf[:need]
	cr.bytesBuf = cr.bytesBuf[need:]
	return data, nil
}

func (cr *compressedReader) uncompressPacket() error {
	header, err := cr.buf.readNext(7) // size of compressed header

	if err != nil {
		return err
	}

	// compressed header structure
	comprLength := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
	uncompressedLength := int(uint32(header[4]) | uint32(header[5])<<8 | uint32(header[6])<<16)
	compressionSequence := uint8(header[3])

	if compressionSequence != cr.mc.compressionSequence {
		return ErrPktSync
	}

	cr.mc.compressionSequence++

	comprData, err := cr.buf.readNext(comprLength)
	if err != nil {
		return err
	}

	// if payload is uncompressed, its length will be specified as zero, and its
	// true length is contained in comprLength
	if uncompressedLength == 0 {
		cr.bytesBuf = append(cr.bytesBuf, comprData...)
		return nil
	}

	// write comprData to a bytes.buffer, then read it using zlib into data
	br := bytes.NewReader(comprData)

	resetter, ok := cr.zr.(zlib.Resetter)

	if ok {
		err := resetter.Reset(br, []byte{})
		if err != nil {
			return err
		}
	} else {
		cr.zr, err = zlib.NewReader(br)
		if err != nil {
			return err
		}
	}

	defer cr.zr.Close()

	//use existing capacity in bytesBuf if possible
	offset := len(cr.bytesBuf)
	if cap(cr.bytesBuf)-offset < uncompressedLength {
		old := cr.bytesBuf
		cr.bytesBuf = make([]byte, offset, offset+uncompressedLength)
		copy(cr.bytesBuf, old)
	}

	data := cr.bytesBuf[offset : offset+uncompressedLength]

	lenRead := 0

	// http://grokbase.com/t/gg/golang-nuts/146y9ppn6b/go-nuts-stream-compression-with-compress-flate
	for lenRead < uncompressedLength {
		n, err := cr.zr.Read(data[lenRead:])
		lenRead += n

		if err == io.EOF {
			if lenRead < uncompressedLength {
				return io.ErrUnexpectedEOF
			}
			break
		}

		if err != nil {
			return err
		}
	}

	cr.bytesBuf = append(cr.bytesBuf, data...)

	return nil
}

func (cw *compressedWriter) Write(data []byte) (int, error) {
	// when asked to write an empty packet, do nothing
	if len(data) == 0 {
		return 0, nil
	}
	totalBytes := len(data)

	length := len(data) - 4

	maxPayloadLength := maxPacketSize - 4

	for length >= maxPayloadLength {
		// cut off a slice of size max payload length
		dataSmall := data[:maxPayloadLength]
		lenSmall := len(dataSmall)

		var b bytes.Buffer
		writer := zlib.NewWriter(&b)
		_, err := writer.Write(dataSmall)
		writer.Close()
		if err != nil {
			return 0, err
		}

		// if compression expands the payload, do not compress
		useData := b.Bytes()

		if len(useData) > len(dataSmall) {
			useData = dataSmall
			lenSmall = 0
		}

		err = cw.writeComprPacketToNetwork(useData, lenSmall)
		if err != nil {
			return 0, err
		}

		length -= maxPayloadLength
		data = data[maxPayloadLength:]
	}

	lenSmall := len(data)

	// do not attempt compression if packet is too small
	if lenSmall < minCompressLength {
		err := cw.writeComprPacketToNetwork(data, 0)
		if err != nil {
			return 0, err
		}

		return totalBytes, nil
	}

	var b bytes.Buffer
	writer := zlib.NewWriter(&b)

	_, err := writer.Write(data)
	writer.Close()

	if err != nil {
		return 0, err
	}

	// if compression expands the payload, do not compress
	useData := b.Bytes()

	if len(useData) > len(data) {
		useData = data
		lenSmall = 0
	}

	err = cw.writeComprPacketToNetwork(useData, lenSmall)

	if err != nil {
		return 0, err
	}
	return totalBytes, nil
}

func (cw *compressedWriter) writeComprPacketToNetwork(data []byte, uncomprLength int) error {
	data = append(cw.header, data...)

	comprLength := len(data) - 7

	// compression header
	data[0] = byte(0xff & comprLength)
	data[1] = byte(0xff & (comprLength >> 8))
	data[2] = byte(0xff & (comprLength >> 16))

	data[3] = cw.mc.compressionSequence

	//this value is never greater than maxPayloadLength
	data[4] = byte(0xff & uncomprLength)
	data[5] = byte(0xff & (uncomprLength >> 8))
	data[6] = byte(0xff & (uncomprLength >> 16))

	if _, err := cw.connWriter.Write(data); err != nil {
		return err
	}

	cw.mc.compressionSequence++
	return nil
}

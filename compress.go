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
}

type compressedWriter struct {
	connWriter io.Writer
	mc         *mysqlConn
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
	}
}

func (cr *compressedReader) readNext(need int) ([]byte, error) {
	for len(cr.bytesBuf) < need {
		err := cr.uncompressPacket()
		if err != nil {
			return nil, err
		}
	}

	data := make([]byte, need)

	copy(data, cr.bytesBuf[:len(data)])

	cr.bytesBuf = cr.bytesBuf[len(data):]

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
	var b bytes.Buffer
	b.Write(comprData)
	r, err := zlib.NewReader(&b)

	if r != nil {
		defer r.Close()
	}

	if err != nil {
		return err
	}

	data := make([]byte, uncompressedLength)
	lenRead := 0

	// http://grokbase.com/t/gg/golang-nuts/146y9ppn6b/go-nuts-stream-compression-with-compress-flate
	for lenRead < uncompressedLength {

		tmp := data[lenRead:]

		n, err := r.Read(tmp)
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

		err = cw.writeComprPacketToNetwork(b.Bytes(), lenSmall)
		if err != nil {
			return 0, err
		}

		length -= maxPayloadLength
		data = data[maxPayloadLength:]
	}

	lenSmall := len(data)

	// do not compress if packet is too small
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

	err = cw.writeComprPacketToNetwork(b.Bytes(), lenSmall)

	if err != nil {
		return 0, err
	}
	return totalBytes, nil
}

func (cw *compressedWriter) writeComprPacketToNetwork(data []byte, uncomprLength int) error {
	data = append([]byte{0, 0, 0, 0, 0, 0, 0}, data...)

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

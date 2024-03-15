// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2024 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"io"
	"os"
	"sync"
)

var (
	zrPool *sync.Pool // Do not use directly. Use zDecompress() instead.
	zwPool *sync.Pool // Do not use directly. Use zCompress() instead.
)

func init() {
	zrPool = &sync.Pool{
		New: func() any { return nil },
	}
	zwPool = &sync.Pool{
		New: func() any {
			zw, err := zlib.NewWriterLevel(new(bytes.Buffer), 2)
			if err != nil {
				panic(err) // compress/zlib return non-nil error only if level is invalid
			}
			return zw
		},
	}
}

func zDecompress(src, dst []byte) (int, error) {
	br := bytes.NewReader(src)
	var zr io.ReadCloser
	var err error

	if a := zrPool.Get(); a == nil {
		if zr, err = zlib.NewReader(br); err != nil {
			return 0, err
		}
	} else {
		zr = a.(io.ReadCloser)
		if zr.(zlib.Resetter).Reset(br, nil); err != nil {
			return 0, err
		}
	}
	defer func() {
		zr.Close()
		zrPool.Put(zr)
	}()

	lenRead := 0
	size := len(dst)

	for lenRead < size {
		n, err := zr.Read(dst[lenRead:])
		lenRead += n

		if err == io.EOF {
			if lenRead < size {
				return lenRead, io.ErrUnexpectedEOF
			}
		} else if err != nil {
			return lenRead, err
		}
	}
	return lenRead, nil
}

func zCompress(src []byte, dst io.Writer) error {
	zw := zwPool.Get().(*zlib.Writer)
	zw.Reset(dst)
	if _, err := zw.Write(src); err != nil {
		return err
	}
	zw.Close()
	zwPool.Put(zw)
	return nil
}

// for debugging wire protocol.
const debugTrace = false

type compressor struct {
	mc         *mysqlConn
	bytesBuf   []byte // read buffer (FIFO)
	connWriter io.Writer
}

func newCompressor(mc *mysqlConn, w io.Writer) *compressor {
	return &compressor{
		mc:         mc,
		connWriter: w,
	}
}

func (c *compressor) readNext(need int) ([]byte, error) {
	for len(c.bytesBuf) < need {
		if err := c.uncompressPacket(); err != nil {
			return nil, err
		}
	}

	data := c.bytesBuf[:need:need] // prevent caller writes into r.bytesBuf
	c.bytesBuf = c.bytesBuf[need:]
	return data, nil
}

func (c *compressor) uncompressPacket() error {
	header, err := c.mc.buf.readNext(7) // size of compressed header
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
	if compressionSequence != c.mc.compresSequence {
		return ErrPktSync
	}
	c.mc.compresSequence++

	comprData, err := c.mc.buf.readNext(comprLength)
	if err != nil {
		return err
	}

	// if payload is uncompressed, its length will be specified as zero, and its
	// true length is contained in comprLength
	if uncompressedLength == 0 {
		c.bytesBuf = append(c.bytesBuf, comprData...)
		return nil
	}

	// use existing capacity in bytesBuf if possible
	offset := len(c.bytesBuf)
	if cap(c.bytesBuf)-offset < uncompressedLength {
		old := c.bytesBuf
		c.bytesBuf = make([]byte, offset, offset+uncompressedLength)
		copy(c.bytesBuf, old)
	}

	lenRead, err := zDecompress(comprData, c.bytesBuf[offset:offset+uncompressedLength])
	if err != nil {
		return err
	}
	if lenRead != uncompressedLength {
		return fmt.Errorf("invalid compressed packet: uncompressed length in header is %d, actual %d",
			uncompressedLength, lenRead)
	}
	c.bytesBuf = c.bytesBuf[:offset+uncompressedLength]
	return nil
}

const maxPayloadLen = maxPacketSize - 4

func (c *compressor) Write(data []byte) (int, error) {
	totalBytes := len(data)
	dataLen := len(data)
	blankHeader := make([]byte, 7)
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
			zCompress(payload, &buf)
		}

		if err := c.writeCompressedPacket(buf.Bytes(), uncompressedLen); err != nil {
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
func (c *compressor) writeCompressedPacket(data []byte, uncompressedLen int) error {
	comprLength := len(data) - 7
	if debugTrace {
		c.mc.cfg.Logger.Print(
			fmt.Sprintf(
				"writeCompressedPacket: comprLength=%v, uncompressedLen=%v, seq=%v",
				comprLength, uncompressedLen, c.mc.compresSequence))
	}

	// compression header
	data[0] = byte(0xff & comprLength)
	data[1] = byte(0xff & (comprLength >> 8))
	data[2] = byte(0xff & (comprLength >> 16))

	data[3] = c.mc.compresSequence

	// this value is never greater than maxPayloadLength
	data[4] = byte(0xff & uncompressedLen)
	data[5] = byte(0xff & (uncompressedLen >> 8))
	data[6] = byte(0xff & (uncompressedLen >> 16))

	if _, err := c.connWriter.Write(data); err != nil {
		c.mc.cfg.Logger.Print(err)
		return err
	}

	c.mc.compresSequence++
	return nil
}

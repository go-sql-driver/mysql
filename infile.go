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
	"database/sql/driver"
	"fmt"
	"io"
	"os"
	"strings"
)

var (
	fileRegister   map[string]bool
	readerRegister map[string]func() io.Reader
)

func init() {
	fileRegister = make(map[string]bool)
	readerRegister = make(map[string]func() io.Reader)
}

// RegisterLocalFile adds the given file to the file whitelist,
// so that it can be used by "LOAD DATA LOCAL INFILE <filepath>".
// Alternatively you can allow the use of all local files with
// the DSN parameter 'allowAllFiles=true'
func RegisterLocalFile(filepath string) {
	fileRegister[filepath] = true
}

// RegisterReader registers a io.Reader so that it can be used by
// "LOAD DATA LOCAL INFILE Reader::<name>".
// The use of io.Reader in this context is NOT safe for concurrency!
func RegisterReaderHandler(name string, cb func() io.Reader) {
	readerRegister[name] = cb
}

func (mc *mysqlConn) handleInFileRequest(name string) (err error) {
	var rdr io.Reader
	data := make([]byte, 4+mc.maxWriteSize)

	if strings.HasPrefix(name, "Reader::") { // io.Reader
		name = name[8:]
		cb, inMap := readerRegister[name]
		if cb != nil {
			rdr = cb()
		}
		if rdr == nil {
			if !inMap {
				err = fmt.Errorf("Reader '%s' is not registered", name)
			} else {
				err = fmt.Errorf("Reader '%s' is <nil>", name)
			}
		}

	} else { // File
		if fileRegister[name] || mc.cfg.params[`allowAllFiles`] == `true` {
			var file *os.File
			file, err = os.Open(name)
			defer file.Close()

			rdr = file
		} else {
			err = fmt.Errorf("Local File '%s' is not registered. Use the DSN parameter 'allowAllFiles=true' to allow all files", name)
		}
	}

	// send content packets
	var ioErr error
	if err == nil {
		var n int
		for err == nil && ioErr == nil {
			n, err = rdr.Read(data[4:])
			if n > 0 {
				data[0] = byte(n)
				data[1] = byte(n >> 8)
				data[2] = byte(n >> 16)
				data[3] = mc.sequence
				ioErr = mc.writePacket(data[:4+n])
			}
		}
		if err == io.EOF {
			err = nil
		}
		if ioErr != nil {
			errLog.Print(ioErr.Error())
			return driver.ErrBadConn
		}
	}

	// send empty packet (termination)
	ioErr = mc.writePacket([]byte{
		0x00,
		0x00,
		0x00,
		mc.sequence,
	})
	if ioErr != nil {
		errLog.Print(ioErr.Error())
		return driver.ErrBadConn
	}

	// read OK packet
	if err == nil {
		return mc.readResultOK()
	} else {
		mc.readPacket()
	}
	return err
}

// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2013 The Go-MySQL-Driver Authors. All rights reserved.
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
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	fileRegister       map[string]bool
	fileRegisterLock   sync.RWMutex
	readerRegister     map[string]func() io.Reader
	readerRegisterLock sync.RWMutex
)

// RegisterLocalFile adds the given file to the file whitelist,
// so that it can be used by "LOAD DATA LOCAL INFILE <filepath>".
// Alternatively you can allow the use of all local files with
// the DSN parameter 'allowAllFiles=true'
//
//  filePath := "/home/gopher/data.csv"
//  mysql.RegisterLocalFile(filePath)
//  err := db.Exec("LOAD DATA LOCAL INFILE '" + filePath + "' INTO TABLE foo")
//  if err != nil {
//  ...
//
func RegisterLocalFile(filePath string) {
	fileRegisterLock.Lock()
	// lazy map init
	if fileRegister == nil {
		fileRegister = make(map[string]bool)
	}

	fileRegister[strings.Trim(filePath, `"`)] = true
	fileRegisterLock.Unlock()
}

// DeregisterLocalFile removes the given filepath from the whitelist.
func DeregisterLocalFile(filePath string) {
	fileRegisterLock.Lock()
	delete(fileRegister, strings.Trim(filePath, `"`))
	fileRegisterLock.Unlock()
}

// RegisterReaderHandler registers a handler function which is used
// to receive a io.Reader.
// The Reader can be used by "LOAD DATA LOCAL INFILE Reader::<name>".
// If the handler returns a io.ReadCloser Close() is called when the
// request is finished.
//
//  mysql.RegisterReaderHandler("data", func() io.Reader {
//  	var csvReader io.Reader // Some Reader that returns CSV data
//  	... // Open Reader here
//  	return csvReader
//  })
//  err := db.Exec("LOAD DATA LOCAL INFILE 'Reader::data' INTO TABLE foo")
//  if err != nil {
//  ...
//
func RegisterReaderHandler(name string, handler func() io.Reader) {
	readerRegisterLock.Lock()
	// lazy map init
	if readerRegister == nil {
		readerRegister = make(map[string]func() io.Reader)
	}

	readerRegister[name] = handler
	readerRegisterLock.Unlock()
}

// DeregisterReaderHandler removes the ReaderHandler function with
// the given name from the registry.
func DeregisterReaderHandler(name string) {
	readerRegisterLock.Lock()
	delete(readerRegister, name)
	readerRegisterLock.Unlock()
}

func deferredClose(err *error, closer io.Closer) {
	closeErr := closer.Close()
	if *err == nil {
		*err = closeErr
	}
}

func (mc *mysqlConn) handleInFileRequest(name string) (err error) {
	var rdr io.Reader
	var data []byte
	packetSize := 16 * 1024 // 16KB is small enough for disk readahead and large enough for TCP
	if mc.maxWriteSize < packetSize {
		packetSize = mc.maxWriteSize
	}

	if name == "Data::Data" {
		return mc.loadDataStart()
	}

	if idx := strings.Index(name, "Reader::"); idx == 0 || (idx > 0 && name[idx-1] == '/') { // io.Reader
		// The server might return an an absolute path. See issue #355.
		name = name[idx+8:]

		readerRegisterLock.RLock()
		handler, inMap := readerRegister[name]
		readerRegisterLock.RUnlock()

		if inMap {
			rdr = handler()
			if rdr != nil {
				if cl, ok := rdr.(io.Closer); ok {
					defer deferredClose(&err, cl)
				}
			} else {
				err = fmt.Errorf("Reader '%s' is <nil>", name)
			}
		} else {
			err = fmt.Errorf("Reader '%s' is not registered", name)
		}
	} else { // File
		name = strings.Trim(name, `"`)
		fileRegisterLock.RLock()
		fr := fileRegister[name]
		fileRegisterLock.RUnlock()
		if mc.cfg.AllowAllFiles || fr {
			var file *os.File
			var fi os.FileInfo

			if file, err = os.Open(name); err == nil {
				defer deferredClose(&err, file)

				// get file size
				if fi, err = file.Stat(); err == nil {
					rdr = file
					if fileSize := int(fi.Size()); fileSize < packetSize {
						packetSize = fileSize
					}
				}
			}
		} else {
			err = fmt.Errorf("local file '%s' is not registered", name)
		}
	}

	// send content packets
	// if packetSize == 0, the Reader contains no data
	if err == nil && packetSize > 0 {
		data := make([]byte, 4+packetSize)
		var n int
		for err == nil {
			n, err = rdr.Read(data[4:])
			if n > 0 {
				if ioErr := mc.writePacket(data[:4+n]); ioErr != nil {
					return ioErr
				}
			}
		}
		if err == io.EOF {
			err = nil
		}
	}

	// send empty packet (termination)
	if data == nil {
		data = make([]byte, 4)
	}
	if ioErr := mc.writePacket(data[:4]); ioErr != nil {
		return ioErr
	}

	// read OK packet
	if err == nil {
		return mc.readResultOK()
	}

	mc.readPacket()
	return err
}

func (mc *mysqlConn) loadDataStart() (err error) {
	mc.inLoadData = true
	mc.maxLoadDataSize = 16 * 1024 // 16KB is small enough for disk readahead and large enough for TCP
	if (mc.maxWriteSize / 2) < mc.maxLoadDataSize {
		mc.maxLoadDataSize = mc.maxWriteSize / 2
	}
	mc.loadData = []byte{0, 0, 0, 0}
	return nil
}

func (mc *mysqlConn) loadDataWrite(args []driver.Value) (err error) {
	if len(args) == 0 {
		return mc.loadDataTerminate()
	}

	for n, column := range args {
		if n > 0 {
			mc.loadData = append(mc.loadData, '\t')
		}
		mc.loadData = mc.appendEncode(mc.loadData, column)
	}
	mc.loadData = append(mc.loadData, '\n')
	if len(mc.loadData) > mc.maxLoadDataSize {
		err = mc.loadDataWritePacket()
		if err != nil {
			return err
		}
	}
	return nil
}

func (mc *mysqlConn) loadDataWritePacket() (err error) {
	if ioErr := mc.writePacket(mc.loadData); ioErr != nil {
		return ioErr
	}
	mc.loadData = mc.loadData[:4]
	return nil
}

func (mc *mysqlConn) loadDataTerminate() (err error) {
	defer func() {
		mc.inLoadData = false
	}()
	if ioErr := mc.loadDataWritePacket(); ioErr != nil {
		return ioErr
	}
	mc.loadData = mc.loadData[:4]
	if ioErr := mc.writePacket(mc.loadData); ioErr != nil {
		return ioErr
	}

	// read OK packet
	if err == nil {
		return mc.readResultOK()
	}

	mc.readPacket()
	return err
}

func (mc *mysqlConn) appendEncode(buf []byte, x driver.Value) []byte {
	switch v := x.(type) {
	case int64:
		return strconv.AppendInt(buf, v, 10)
	case uint64:
		return strconv.AppendUint(buf, v, 10)
	case float64:
		return strconv.AppendFloat(buf, v, 'g', -1, 64)
	case bool:
		if v {
			return append(buf, '1')
		} else {
			return append(buf, '0')
		}
	case time.Time:
		if v.IsZero() {
			return append(buf, "0000-00-00"...)
		} else {
			v := v.In(mc.cfg.Loc)
			v = v.Add(time.Nanosecond * 500) // To round under microsecond
			year := v.Year()
			year100 := year / 100
			year1 := year % 100
			month := v.Month()
			day := v.Day()
			hour := v.Hour()
			minute := v.Minute()
			second := v.Second()
			micro := v.Nanosecond() / 1000

			buf := append(buf, []byte{
				digits10[year100], digits01[year100],
				digits10[year1], digits01[year1],
				'-',
				digits10[month], digits01[month],
				'-',
				digits10[day], digits01[day],
				' ',
				digits10[hour], digits01[hour],
				':',
				digits10[minute], digits01[minute],
				':',
				digits10[second], digits01[second],
			}...)
			if micro != 0 {
				micro10000 := micro / 10000
				micro100 := micro / 100 % 100
				micro1 := micro % 100
				buf = append(buf, []byte{
					'.',
					digits10[micro10000], digits01[micro10000],
					digits10[micro100], digits01[micro100],
					digits10[micro1], digits01[micro1],
				}...)
			}
			return buf
		}
	case []byte:
		if v == nil {
			return append(buf, "\\N"...)
		} else {
			if mc.status&statusNoBackslashEscapes == 0 {
				buf = escapeBytesBackslash(buf, v)
			} else {
				buf = escapeBytesQuotes(buf, v)
			}
			return buf
		}
	case string:
		return appendEscaped(buf, v)
	case nil:
		return append(buf, "\\N"...)
	default:
		errLog.Print("unsupported type")
		return buf
	}
}

func appendEscaped(buf []byte, v string) []byte {
	escapeNeeded := false
	startPos := 0
	var c byte

	for i := 0; i < len(v); i++ {
		c = v[i]
		if c == '\\' || c == '\n' || c == '\r' || c == '\t' {
			escapeNeeded = true
			startPos = i
			break
		}
	}
	if !escapeNeeded {
		return append(buf, v...)
	}

	result := append(buf, v[:startPos]...)
	for i := startPos; i < len(v); i++ {
		c = v[i]
		switch c {
		case '\\':
			result = append(result, '\\', '\\')
		case '\n':
			result = append(result, '\\', 'n')
		case '\r':
			result = append(result, '\\', 'r')
		case '\t':
			result = append(result, '\\', 't')
		default:
			result = append(result, c)
		}
	}
	return result
}

// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2012 Julien Schmidt. All rights reserved.
// http://www.julienschmidt.com
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"database/sql/driver"
	"io"
)

type mysqlField struct {
	name      string
	fieldType byte
	flags     fieldFlag
}

// mysqlRows is the driver-internal Rows struct that is never given to
// the database/sql package. This struct is 40 bytes on 64-bit
// machines and is recycled. Its size isn't very relevant, since we
// recycle it.
//
// Allocate with newMysqlRows (from buffer.go) and return with
// putMySQLRows.  See also: mysqlRowsI.
type mysqlRows struct {
	mc      *mysqlConn
	columns []mysqlField
	binary  bool // Note: packing small bool fields at the end
	eof     bool
}

// mysqlRowsI implements driver.Rows. Its wrapped *mysqlRows pointer
// becomes nil and recycled on Close. This struct is kept small (8
// bytes) to minimize garbage creation.
type mysqlRowsI struct {
	*mysqlRows
}

func (rows *mysqlRows) Columns() (columns []string) {
	if rows == nil {
		println("mysql-driver: mysqlRows.Columns called with nil receiver")
		return nil
	}
	columns = make([]string, len(rows.columns))
	for i := range columns {
		columns[i] = rows.columns[i].name
	}
	return
}

func (ri *mysqlRowsI) Close() (err error) {
	if ri.mysqlRows == nil {
		errLog.Print("mysqlRows.Close called twice? sql package fail?")
		return errInvConn
	}
	err = ri.mysqlRows.close()
	putMysqlRows(ri.mysqlRows)
	ri.mysqlRows = nil
	return err
}

func (rows *mysqlRows) close() (err error) {
	defer func() {
		rows.mc = nil
		putMysqlFields(rows.columns)
		rows.columns = nil
	}()

	// Remove unread packets from stream
	if !rows.eof {
		if rows.mc == nil {
			return errInvConn
		}

		err = rows.mc.readUntilEOF()
	}

	return
}

func (rows *mysqlRows) Next(dest []driver.Value) error {
	if rows == nil {
		errLog.Print("mysqlRows.Next called with nil receiver")
		return errInvConn
	}
	if rows.eof {
		return io.EOF
	}

	if rows.mc == nil {
		return errInvConn
	}

	// Fetch next row from stream
	var err error
	if rows.binary {
		err = rows.readBinaryRow(dest)
	} else {
		err = rows.readRow(dest)
	}

	if err == io.EOF {
		rows.eof = true
	}
	return err
}

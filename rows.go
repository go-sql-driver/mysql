// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2012 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"database/sql"
	"database/sql/driver"
	"io"
	"reflect"
	"time"
)

var (
	scanTypeNil        = reflect.TypeOf(nil)
	scanTypeNullInt    = reflect.TypeOf(sql.NullInt64{})
	scanTypeUint8      = reflect.TypeOf(uint8(0))
	scanTypeInt8       = reflect.TypeOf(int8(0))
	scanTypeUint16     = reflect.TypeOf(uint16(0))
	scanTypeInt16      = reflect.TypeOf(int16(0))
	scanTypeUint32     = reflect.TypeOf(uint32(0))
	scanTypeInt32      = reflect.TypeOf(int32(0))
	scanTypeUint64     = reflect.TypeOf(uint64(0))
	scanTypeInt64      = reflect.TypeOf(int64(0))
	scanTypeNullFloat  = reflect.TypeOf(sql.NullFloat64{})
	scanTypeFloat32    = reflect.TypeOf(float32(0))
	scanTypeFloat64    = reflect.TypeOf(float64(0))
	scanTypeNullString = reflect.TypeOf(sql.NullString{})
	scanTypeString     = reflect.TypeOf("")
	scanTypeBytes      = reflect.TypeOf([]byte{})
	scanTypeRawBytes   = reflect.TypeOf(sql.RawBytes{})
	scanTypeTime       = reflect.TypeOf(time.Time{})
	scanTypeUnknown    = reflect.TypeOf(new(interface{}))
)

type mysqlField struct {
	tableName string
	name      string
	flags     fieldFlag
	fieldType byte
	decimals  byte
}

func (mf *mysqlField) scanType() reflect.Type {
	switch mf.fieldType {
	case fieldTypeNULL:
		return scanTypeNil

	case fieldTypeTiny:
		if mf.flags&flagNotNULL != 0 {
			if mf.flags&flagUnsigned != 0 {
				return scanTypeUint8
			}
			return scanTypeInt8
		}
		return scanTypeNullInt

	case fieldTypeShort, fieldTypeYear:
		if mf.flags&flagNotNULL != 0 {
			if mf.flags&flagUnsigned != 0 {
				return scanTypeUint16
			}
			return scanTypeInt16
		}
		return scanTypeNullInt

	case fieldTypeInt24, fieldTypeLong:
		if mf.flags&flagNotNULL != 0 {
			if mf.flags&flagUnsigned != 0 {
				return scanTypeUint32
			}
			return scanTypeInt32
		}
		return scanTypeNullInt

	case fieldTypeLongLong:
		if mf.flags&flagNotNULL != 0 {
			if mf.flags&flagUnsigned != 0 {
				return scanTypeUint64
			}
			return scanTypeInt64
		}
		return scanTypeNullInt

	case fieldTypeFloat:
		if mf.flags&flagNotNULL != 0 {
			return scanTypeFloat32
		}
		return scanTypeNullFloat

	case fieldTypeDouble:
		if mf.flags&flagNotNULL != 0 {
			return scanTypeFloat64
		}
		return scanTypeNullFloat

	case fieldTypeDecimal, fieldTypeNewDecimal, fieldTypeVarChar,
		fieldTypeBit, fieldTypeEnum, fieldTypeSet, fieldTypeTinyBLOB,
		fieldTypeMediumBLOB, fieldTypeLongBLOB, fieldTypeBLOB,
		fieldTypeVarString, fieldTypeString, fieldTypeGeometry, fieldTypeJSON,
		fieldTypeTime:
		if mf.flags&flagNotNULL != 0 {
			// alternatively we could return []byte or even RawBytes
			return scanTypeString
		}
		return scanTypeNullString

	case
		fieldTypeDate, fieldTypeNewDate,
		fieldTypeTimestamp, fieldTypeDateTime:

		// TODO: NULL
		// TODO: respect rows.mc.parseTime
		return scanTypeTime

	default:
		return scanTypeUnknown
	}
}

type resultSet struct {
	columns     []mysqlField
	columnNames []string
	done        bool
}

type mysqlRows struct {
	mc     *mysqlConn
	rs     resultSet
	finish func()
}

type binaryRows struct {
	mysqlRows
}

type textRows struct {
	mysqlRows
}

func (rows *mysqlRows) Columns() []string {
	if rows.rs.columnNames != nil {
		return rows.rs.columnNames
	}

	columns := make([]string, len(rows.rs.columns))
	if rows.mc != nil && rows.mc.cfg.ColumnsWithAlias {
		for i := range columns {
			if tableName := rows.rs.columns[i].tableName; len(tableName) > 0 {
				columns[i] = tableName + "." + rows.rs.columns[i].name
			} else {
				columns[i] = rows.rs.columns[i].name
			}
		}
	} else {
		for i := range columns {
			columns[i] = rows.rs.columns[i].name
		}
	}

	rows.rs.columnNames = columns
	return columns
}

func (rows *mysqlRows) ColumnTypeScanType(i int) reflect.Type {
	return rows.rs.columns[i].scanType()
}

func (rows *mysqlRows) Close() (err error) {
	if f := rows.finish; f != nil {
		f()
		rows.finish = nil
	}

	mc := rows.mc
	if mc == nil {
		return nil
	}
	if err := mc.error(); err != nil {
		return err
	}

	// Remove unread packets from stream
	if !rows.rs.done {
		err = mc.readUntilEOF()
	}
	if err == nil {
		if err = mc.discardResults(); err != nil {
			return err
		}
	}

	rows.mc = nil
	return err
}

func (rows *mysqlRows) HasNextResultSet() (b bool) {
	if rows.mc == nil {
		return false
	}
	return rows.mc.status&statusMoreResultsExists != 0
}

func (rows *mysqlRows) nextResultSet() (int, error) {
	if rows.mc == nil {
		return 0, io.EOF
	}
	if err := rows.mc.error(); err != nil {
		return 0, err
	}

	// Remove unread packets from stream
	if !rows.rs.done {
		if err := rows.mc.readUntilEOF(); err != nil {
			return 0, err
		}
		rows.rs.done = true
	}

	if !rows.HasNextResultSet() {
		rows.mc = nil
		return 0, io.EOF
	}
	rows.rs = resultSet{}
	return rows.mc.readResultSetHeaderPacket()
}

func (rows *mysqlRows) nextNotEmptyResultSet() (int, error) {
	for {
		resLen, err := rows.nextResultSet()
		if err != nil {
			return 0, err
		}

		if resLen > 0 {
			return resLen, nil
		}

		rows.rs.done = true
	}
}

func (rows *binaryRows) NextResultSet() error {
	resLen, err := rows.nextNotEmptyResultSet()
	if err != nil {
		return err
	}

	rows.rs.columns, err = rows.mc.readColumns(resLen)
	return err
}

func (rows *binaryRows) Next(dest []driver.Value) error {
	if mc := rows.mc; mc != nil {
		if err := mc.error(); err != nil {
			return err
		}

		// Fetch next row from stream
		return rows.readRow(dest)
	}
	return io.EOF
}

func (rows *textRows) NextResultSet() (err error) {
	resLen, err := rows.nextNotEmptyResultSet()
	if err != nil {
		return err
	}

	rows.rs.columns, err = rows.mc.readColumns(resLen)
	return err
}

func (rows *textRows) Next(dest []driver.Value) error {
	if mc := rows.mc; mc != nil {
		if err := mc.error(); err != nil {
			return err
		}

		// Fetch next row from stream
		return rows.readRow(dest)
	}
	return io.EOF
}

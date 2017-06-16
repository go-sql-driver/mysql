// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2012 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"database/sql/driver"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"time"
	"unsafe"
)

type mysqlStmt struct {
	mc         *mysqlConn
	id         uint32
	paramCount int
}

func (stmt *mysqlStmt) Close() error {
	if stmt.mc == nil || stmt.mc.closed.IsSet() {
		// driver.Stmt.Close can be called more than once, thus this function
		// has to be idempotent.
		// See also Issue #450 and golang/go#16019.
		//errLog.Print(ErrInvalidConn)
		return driver.ErrBadConn
	}

	err := stmt.mc.writeCommandPacketUint32(comStmtClose, stmt.id)
	stmt.mc = nil
	return err
}

func (stmt *mysqlStmt) NumInput() int {
	return stmt.paramCount
}

func (stmt *mysqlStmt) ColumnConverter(idx int) driver.ValueConverter {
	return &converter{}
}

func (stmt *mysqlStmt) Exec(args []driver.Value) (driver.Result, error) {
	if stmt.mc.closed.IsSet() {
		errLog.Print(ErrInvalidConn)
		return nil, driver.ErrBadConn
	}
	// Send command
	err := stmt.writeExecutePacket(args)
	if err != nil {
		return nil, err
	}

	mc := stmt.mc

	mc.affectedRows = 0
	mc.insertId = 0

	// Read Result
	resLen, err := mc.readResultSetHeaderPacket()
	if err != nil {
		return nil, err
	}

	if resLen > 0 {
		// Columns
		if err = mc.readUntilEOF(); err != nil {
			return nil, err
		}

		// Rows
		if err := mc.readUntilEOF(); err != nil {
			return nil, err
		}
	}

	if err := mc.discardResults(); err != nil {
		return nil, err
	}

	return &mysqlResult{
		affectedRows: int64(mc.affectedRows),
		insertId:     int64(mc.insertId),
	}, nil
}

func (stmt *mysqlStmt) Query(args []driver.Value) (driver.Rows, error) {
	return stmt.query(args)
}

func (stmt *mysqlStmt) query(args []driver.Value) (*binaryRows, error) {
	if stmt.mc.closed.IsSet() {
		errLog.Print(ErrInvalidConn)
		return nil, driver.ErrBadConn
	}
	// Send command
	err := stmt.writeExecutePacket(args)
	if err != nil {
		return nil, err
	}

	mc := stmt.mc

	// Read Result
	resLen, err := mc.readResultSetHeaderPacket()
	if err != nil {
		return nil, err
	}

	rows := new(binaryRows)

	if resLen > 0 {
		rows.mc = mc
		rows.rs.columns, err = mc.readColumns(resLen)
	} else {
		rows.rs.done = true

		switch err := rows.NextResultSet(); err {
		case nil, io.EOF:
			return rows, nil
		default:
			return nil, err
		}
	}

	return rows, err
}

type converter struct{}

func (c *converter) ConvertValue(v interface{}) (driver.Value, error) {
	if driver.IsValue(v) {
		return v, nil
	}

	if UnsafePointerOptimization {
		switch x := v.(type) {
		case *int64, *float64, *bool, *string, *[]byte, *time.Time:
			return hack(x), nil
		}
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Ptr:
		// indirect pointers
		if rv.IsNil() {
			return nil, nil
		}
		return c.ConvertValue(rv.Elem().Interface())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return int64(rv.Uint()), nil
	case reflect.Uint64:
		u64 := rv.Uint()
		if u64 >= 1<<63 {
			return strconv.FormatUint(u64, 10), nil
		}
		return int64(u64), nil
	case reflect.Float32, reflect.Float64:
		return rv.Float(), nil
	}
	return nil, fmt.Errorf("unsupported type %T, a %s", v, rv.Kind())
}

// UnsafePointerOptimization may be set to true to enable a pass-by-pointer
// optimization that prevents dynamic memory allocations when converting values
// between concrete types and driver.Value (which is an empty interface).
//
// Enabling this optimization may not be be portable and may cause the program
// to crash in future versions of Go, use it at your own risks!
var UnsafePointerOptimization = false

type eface struct {
	t uintptr
	v uintptr
}

func (e eface) value() (i interface{}) {
	*((*eface)(unsafe.Pointer(&i))) = e
	return
}

const vmask = uintptr(1 << 63)

func typeval(v interface{}) eface {
	return *(*eface)(unsafe.Pointer(&v))
}

func typeof(v interface{}) uintptr {
	return typeval(v).t
}

func hack(v interface{}) interface{} {
	if UnsafePointerOptimization {
		e := typeval(v)
		e.t = valOf(e.t)
		e.v |= vmask
		v = e.value()
	}
	return v
}

func unhack(v interface{}) interface{} {
	if UnsafePointerOptimization {
		e := typeval(v)

		if (e.v & vmask) == 0 {
			return v
		}

		e.t = ptrOf(e.t)
		e.v &= ^vmask
		v = e.value()
	}
	return v
}

func ptrOf(t uintptr) uintptr {
	switch t {
	case int64Type:
		return int64PtrType
	case float64Type:
		return float64PtrType
	case boolType:
		return boolPtrType
	case stringType:
		return stringPtrType
	case bytesType:
		return bytesPtrType
	case timeType:
		return timePtrType
	default:
		panic("unsupported type passed to ptrOf")
	}
}

func valOf(t uintptr) uintptr {
	switch t {
	case int64PtrType:
		return int64Type
	case float64PtrType:
		return float64Type
	case boolPtrType:
		return boolType
	case stringPtrType:
		return stringType
	case bytesPtrType:
		return bytesType
	case timePtrType:
		return timeType
	default:
		panic("unsupported type passed to valOf")
	}
}

var (
	int64Type    = typeof(int64(0))
	int64PtrType = typeof(new(int64))

	float64Type    = typeof(float64(0))
	float64PtrType = typeof(new(float64))

	boolType    = typeof(false)
	boolPtrType = typeof(new(bool))

	stringType    = typeof(string(0))
	stringPtrType = typeof(new(string))

	bytesType    = typeof([]byte(nil))
	bytesPtrType = typeof(new([]byte))

	timeType    = typeof(time.Time{})
	timePtrType = typeof(new(time.Time))
)

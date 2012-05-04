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
	"fmt"
	"reflect"
	"time"
)

type stmtContent struct {
	mc             *mysqlConn
	id             uint32
	query          string
	paramCount     int
	params         []*mysqlField
	args           *[]driver.Value
	newParamsBound bool
}

type mysqlStmt struct {
	*stmtContent
}

func (stmt mysqlStmt) Close() error {
	e := stmt.mc.writeCommandPacket(COM_STMT_CLOSE, stmt.id)
	stmt.params = nil
	stmt.mc = nil
	return e
}

func (stmt mysqlStmt) NumInput() int {
	return stmt.paramCount
}

func (stmt mysqlStmt) Exec(args []driver.Value) (driver.Result, error) {
	stmt.mc.affectedRows = 0
	stmt.mc.insertId = 0
	
	// Send command
	e := stmt.buildExecutePacket(&args)
	if e != nil {
		return nil, e
	}

	// Read Result
	var resLen int
	resLen, e = stmt.mc.readResultSetHeaderPacket()
	if e != nil {
		return nil, e
	}

	if resLen > 0 {
		_, e = stmt.mc.readUntilEOF()
		if e != nil {
			return nil, e
		}

		stmt.mc.affectedRows, e = stmt.mc.readUntilEOF()
		if e != nil {
			return nil, e
		}
	}
	if e != nil {
		return nil, e
	}

	if stmt.mc.affectedRows == 0 {
		return driver.ResultNoRows, nil
	}

	return &mysqlResult{
			affectedRows: int64(stmt.mc.affectedRows),
			insertId:     int64(stmt.mc.insertId)},
		nil
}

func (stmt mysqlStmt) Query(args []driver.Value) (dr driver.Rows, e error) {
	// Send command
	e = stmt.buildExecutePacket(&args)
	if e != nil {
		return nil, e
	}

	// Get Result
	var resLen int
	rows := new(mysqlRows)
	rows.content = new(rowsContent)
	resLen, e = stmt.mc.readResultSetHeaderPacket()
	if e != nil {
		return nil, e
	}

	if resLen > 0 {
		// Columns
		rows.content.columns, e = stmt.mc.readColumns(resLen)
		if e != nil {
			return
		}

		// Rows
		e = stmt.mc.readBinaryRows(rows.content)
		if e != nil {
			return
		}
	}

	dr = rows
	return
}

/* Command Packet
Bytes                Name
-----                ----
1                    code
4                    statement_id
1                    flags
4                    iteration_count
  if param_count > 0:
(param_count+7)/8    null_bit_map
1                    new_parameter_bound_flag
  if new_params_bound == 1:
n*2                  type of parameters
n                    values for the parameters 
*/
func (stmt mysqlStmt) buildExecutePacket(args *[]driver.Value) (e error) {
	if len(*args) < stmt.paramCount {
		return fmt.Errorf(
			"Not enough Arguments to call STMT_EXEC (Got: %d Has: %d",
			len(*args),
			stmt.paramCount)
	}
	
	// Reset packet-sequence
	stmt.mc.sequence = 0

	data := make([]byte, 0, 10)

	// code [1 byte]
	data = append(data, byte(COM_STMT_EXECUTE))

	// statement_id [4 bytes]
	data = append(data, uint32ToBytes(stmt.id)...)

	// flags (0: CURSOR_TYPE_NO_CURSOR) [1 byte]
	data = append(data, byte(0))

	// iteration_count [4 bytes]
	data = append(data, uint32ToBytes(1)...)

	if stmt.paramCount > 0 {
		var i int
		
		// build nullBitMap
		nullBitMap := make([]byte, (stmt.paramCount+7)/8)
		bitMask := uint64(0)

		// Check for NULL fields
		for i = 0; i < stmt.paramCount; i++ {
			if (*args)[i] == nil {
				fmt.Println("nil", i, (*args)[i])
				bitMask += 1 << uint(i)
			}
		}
		// Convert bitMask to bytes
		for i = 0; i < len(nullBitMap); i++ {
			nullBitMap[i] = byte(bitMask >> uint(i*8))
		}

		// append nullBitMap [(param_count+7)/8 bytes]
		data = append(data, nullBitMap...)

		// Check for changed Params
		newParamsBound := true
		if stmt.args != nil {
			for i := 0; i < len(*args); i++ {
				if (*args)[i] != (*stmt.args)[i] {
					fmt.Println((*args)[i], "!=", (*stmt.args)[i])
					newParamsBound = false
					break
				}
			}
		}

		// No (new) Parameters bound or rebound
		if !newParamsBound {
			//newParameterBoundFlag 0 [1 byte]
			data = append(data, byte(0))
		} else {
			// newParameterBoundFlag 1 [1 byte]
			data = append(data, byte(1))

			// append types and cache values
			paramValues := make([]byte, 0)
			var pv reflect.Value
			for i = 0; i < stmt.paramCount; i++ {
				switch (*args)[i].(type) {
				case nil:
					data = append(data, []byte{
						byte(FIELD_TYPE_NULL),
						0x0}...)
					continue
				case []byte:
					fmt.Println("[]byte", (*args)[i])
				case time.Time:
					fmt.Println("time.Time", (*args)[i])
				}

				pv = reflect.ValueOf((*args)[i])
				switch pv.Kind() {
				case reflect.Int64:
					data = append(data, []byte{
					byte(FIELD_TYPE_LONGLONG),
					0x0}...)
					paramValues = append(paramValues, int64ToBytes(pv.Int())...)
					fmt.Println("int64", (*args)[i])

				case reflect.Float64:
					fmt.Println("float64", (*args)[i])

				case reflect.Bool:
					data = append(data, []byte{
					byte(FIELD_TYPE_TINY),
					0x0}...)
					val := pv.Bool()
					if val {
						paramValues = append(paramValues, byte(1))
					} else {
						paramValues = append(paramValues, byte(0))
					}
					fmt.Println("bool", (*args)[i])

				case reflect.String:
					data = append(data, []byte{
					byte(FIELD_TYPE_STRING),
					0x0}...)
					val := pv.String()
					paramValues = append(paramValues, lengthCodedBinaryToBytes(uint64(len(val)))...)
					paramValues = append(paramValues, []byte(val)...)
					fmt.Println("string", string([]byte(val)))

				default:
					return fmt.Errorf("Invalid Value: %s", pv.Kind().String())
				}
			}
			
			// append cached values
			data = append(data, paramValues...)
			fmt.Println("data", string(data))
		}

		// Save args
		stmt.args = args
	}
	return stmt.mc.writePacket(data)
}

// ColumnConverter returns a ValueConverter for the provided
// column index.  If the type of a specific column isn't known
// or shouldn't be handled specially, DefaultValueConverter
// can be returned.
func (stmt mysqlStmt) ColumnConverter(idx int) driver.ValueConverter {
	debug(fmt.Sprintf("ColumnConverter(%d)", idx))
	return driver.DefaultParameterConverter
}

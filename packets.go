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
	"errors"
	"fmt"
	"reflect"
	"time"
)

// Packets documentation:
// http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol

// Read packet to buffer 'data'
func (mc *mysqlConn) readPacket() ([]byte, error) {
	// Packet Length
	pktLen, e := mc.readNumber(3)
	if e != nil {
		return nil, e
	}

	if int(pktLen) == 0 {
		return nil, e
	}

	// Packet Number
	pktSeq, e := mc.readNumber(1)
	if e != nil {
		return nil, e
	}

	// Check Packet Sync
	if uint8(pktSeq) != mc.sequence {
		e = errors.New("Commands out of sync; you can't run this command now")
		return nil, e
	}
	mc.sequence++

	// Read rest of packet
	data := make([]byte, pktLen)
	var n, add int
	for e == nil && n < int(pktLen) {
		add, e = mc.bufReader.Read(data[n:])
		n += add
	}
	if e != nil || n < int(pktLen) {
		if e == nil {
			e = fmt.Errorf("Length of read data (%d) does not match body length (%d)", n, pktLen)
		}
		errLog.Print(`packets:58 `, e)
		return nil, driver.ErrBadConn
	}
	return data, e
}

// Read n bytes long number num
func (mc *mysqlConn) readNumber(nr uint8) (uint64, error) {
	// Read bytes into array
	buf := make([]byte, nr)
	var n, add int
	var e error
	for e == nil && n < int(nr) {
		add, e = mc.bufReader.Read(buf[n:])
		n += add
	}
	if e != nil || n < int(nr) {
		if e == nil {
			e = fmt.Errorf("Length of read data (%d) does not match header length (%d)", n, nr)
		}
		errLog.Print(`packets:78 `, e)
		return 0, driver.ErrBadConn
	}

	// Convert to uint64
	var num uint64 = 0
	for i := uint8(0); i < nr; i++ {
		num |= uint64(buf[i]) << (i * 8)
	}
	return num, e
}

func (mc *mysqlConn) writePacket(data *[]byte) error {
	// Set time BEFORE to avoid possible collisions
	if mc.server.keepalive > 0 {
		mc.lastCmdTime = time.Now()
	}

	// Write packet
	n, e := mc.netConn.Write(*data)
	if e != nil || n != len(*data) {
		if e == nil {
			e = errors.New("Length of send data does not match packet length")
		}
		errLog.Print(`packets:102 `, e)
		return driver.ErrBadConn
	}

	mc.sequence++
	return nil
}

/******************************************************************************
*                           Initialisation Process                            *
******************************************************************************/

/* Handshake Initialization Packet 
 Bytes                        Name
 -----                        ----
 1                            protocol_version
 n (Null-Terminated String)   server_version
 4                            thread_id
 8                            scramble_buff
 1                            (filler) always 0x00
 2                            server_capabilities
 1                            server_language
 2                            server_status
 2                            server capabilities (two upper bytes)
 1                            length of the scramble
10                            (filler)  always 0
 n                            rest of the plugin provided data (at least 12 bytes) 
 1                            \0 byte, terminating the second part of a scramble
*/
func (mc *mysqlConn) readInitPacket() (e error) {
	data, e := mc.readPacket()
	if e != nil {
		return
	}

	mc.server = new(serverSettings)

	// Position
	pos := 0

	// Protocol version [8 bit uint]
	mc.server.protocol = data[pos]
	if mc.server.protocol < MIN_PROTOCOL_VERSION {
		e = fmt.Errorf(
			"Unsupported MySQL Protocol Version %d. Protocol Version %d or higher is required",
			mc.server.protocol,
			MIN_PROTOCOL_VERSION)
	}
	pos++

	// Server version [null terminated string]
	slice, err := readSlice(data[pos:], 0x00)
	if err != nil {
		return
	}
	mc.server.version = string(slice)
	pos += len(slice) + 1

	// Thread id [32 bit uint]
	mc.server.threadID = bytesToUint32(data[pos : pos+4])
	pos += 4

	// First part of scramble buffer [8 bytes]
	mc.server.scrambleBuff = make([]byte, 8)
	mc.server.scrambleBuff = data[pos : pos+8]
	pos += 9

	// Server capabilities [16 bit uint]
	mc.server.flags = ClientFlag(bytesToUint16(data[pos : pos+2]))
	if mc.server.flags&CLIENT_PROTOCOL_41 == 0 {
		e = errors.New("MySQL-Server does not support required Protocol 41+")
	}
	pos += 2

	// Server language [8 bit uint]
	mc.server.charset = data[pos]
	pos++

	// Server status [16 bit uint]
	pos += 15

	mc.server.scrambleBuff = append(mc.server.scrambleBuff, data[pos:pos+12]...)

	return
}

/* Client Authentication Packet 
Bytes                        Name
-----                        ----
4                            client_flags
4                            max_packet_size
1                            charset_number
23                           (filler) always 0x00...
n (Null-Terminated String)   user
n (Length Coded Binary)      scramble_buff (1 + x bytes)
n (Null-Terminated String)   databasename (optional)
*/
func (mc *mysqlConn) writeAuthPacket() (e error) {
	// Adjust client flags based on server support
	clientFlags := uint32(CLIENT_MULTI_STATEMENTS |
		// CLIENT_MULTI_RESULTS |
		CLIENT_PROTOCOL_41 |
		CLIENT_SECURE_CONN |
		CLIENT_LONG_PASSWORD |
		CLIENT_TRANSACTIONS)
	if mc.server.flags&CLIENT_LONG_FLAG > 0 {
		clientFlags |= uint32(CLIENT_LONG_FLAG)
	}

	// User Password
	scrambleBuff := scramblePassword(mc.server.scrambleBuff, []byte(mc.cfg.passwd))

	pktLen := 4 + 4 + 1 + 23 + len(mc.cfg.user) + 1 + 1 + len(scrambleBuff) 

	// To specify a db name
	if len(mc.cfg.dbname) > 0 {
		clientFlags |= uint32(CLIENT_CONNECT_WITH_DB)
		pktLen += len(mc.cfg.dbname) + 1 
	}

	// Calculate packet length and make buffer with that size
	data := make([]byte, 0, pktLen+4)

	// Add the packet header
	data = append(data, uint24ToBytes(uint32(pktLen))...)
	data = append(data, mc.sequence)

	// ClientFlags
	data = append(data, uint32ToBytes(clientFlags)...)

	// MaxPacketSize
	data = append(data, uint32ToBytes(MAX_PACKET_SIZE)...)

	// Charset
	data = append(data, mc.server.charset)

	// Filler
	data = append(data, make([]byte, 23)...)

	// User
	if len(mc.cfg.user) > 0 {
		data = append(data, []byte(mc.cfg.user)...)
	}

	// Null-Terminator
	data = append(data, 0x0)

	// ScrambleBuffer
	data = append(data, byte(len(scrambleBuff)))
	if len(scrambleBuff) > 0 {
		data = append(data, scrambleBuff...)
	}

	// Databasename
	if len(mc.cfg.dbname) > 0 {
		data = append(data, []byte(mc.cfg.dbname)...)
		// Null-Terminator
		data = append(data, 0x0)
	}

	// Send Auth packet
	return mc.writePacket(&data)
}

/******************************************************************************
*                             Command Packets                                 *
******************************************************************************/

/* Command Packet
Bytes                        Name
-----                        ----
1                            command
n                            arg
*/
func (mc *mysqlConn) writeCommandPacket(command commandType, args ...interface{}) (e error) {
	// Reset Packet Sequence
	mc.sequence = 0

	var arg []byte

	switch command {

	// Commands without args
	case COM_QUIT, COM_PING:
		if len(args) > 0 {
			return fmt.Errorf("Too much arguments (Got: %d Has: 0)", len(args))
		}
		arg = []byte{}

	// Commands with 1 arg unterminated string
	case COM_QUERY, COM_STMT_PREPARE:
		if len(args) != 1 {
			return fmt.Errorf("Invalid arguments count (Got: %d Has: 1)", len(args))
		}
		arg = []byte(args[0].(string))

	// Commands with 1 arg 32 bit uint
	case COM_STMT_CLOSE:
		if len(args) != 1 {
			return fmt.Errorf("Invalid arguments count (Got: %d Has: 1)", len(args))
		}
		arg = uint32ToBytes(args[0].(uint32))

	default:
		return fmt.Errorf("Unknown command: %d", command)
	}

	pktLen := 1 + len(arg)
	data := make([]byte, 0, pktLen+4)

	// Add the packet header
	data = append(data, uint24ToBytes(uint32(pktLen))...)
	data = append(data, mc.sequence)

	// Add command byte
	data = append(data, byte(command))

	// Add arg
	data = append(data, arg...)

	// Send CMD packet
	return mc.writePacket(&data)
}

/******************************************************************************
*                              Result Packets                                 *
******************************************************************************/

// Returns error if Packet is not an 'Result OK'-Packet
func (mc *mysqlConn) readResultOK() (e error) {
	data, e := mc.readPacket()
	if e != nil {
		return
	}

	switch data[0] {
	// OK
	case 0:
		return mc.handleOkPacket(data)
	// EOF, someone is using old_passwords
	case 254:
		e = errors.New("It seems like you are using old_passwords, which is unsupported. See http://code.google.com/p/go-mysql-driver/wiki/old_passwords")
		return
	// ERROR
	case 255:
		return mc.handleErrorPacket(data)
	default:
		e = errors.New("Invalid Result Packet-Type")
		return
	}

	return
}

/* Error Packet 
Bytes                       Name
-----                       ----
1                           field_count, always = 0xff
2                           errno
1                           (sqlstate marker), always '#'
5                           sqlstate (5 characters)
n                           message
*/
func (mc *mysqlConn) handleErrorPacket(data []byte) (e error) {
	if data[0] != 255 {
		e = errors.New("Wrong Packet-Type: Not an Error-Packet")
		return
	}

	pos := 1

	// Error Number [16 bit uint]
	errno := bytesToUint16(data[pos : pos+2])
	pos += 2

	// SQL State [# + 5bytes string]
	//sqlstate := string(data[pos : pos+6])
	pos += 6

	// Error Message [string]
	message := string(data[pos:])

	e = fmt.Errorf("Error %d: %s", errno, message)
	return
}

/* Ok Packet 
Bytes                       Name
-----                       ----
1   (Length Coded Binary)   field_count, always = 0
1-9 (Length Coded Binary)   affected_rows
1-9 (Length Coded Binary)   insert_id
2                           server_status
2                           warning_count
n   (until end of packet)   message
*/
func (mc *mysqlConn) handleOkPacket(data []byte) (e error) {
	if data[0] != 0 {
		e = errors.New("Wrong Packet-Type: Not an OK-Packet")
		return
	}

	// Position
	pos := 1

	// Affected rows [Length Coded Binary]
	affectedRows, n, e := bytesToLengthCodedBinary(data[pos:])
	if e != nil {
		return
	}
	pos += n

	// Insert id [Length Coded Binary]
	insertID, n, e := bytesToLengthCodedBinary(data[pos:])
	if e != nil {
		return
	}

	// Skip remaining data

	mc.affectedRows = affectedRows
	mc.insertId = insertID

	return
}

/* Result Set Header Packet 
 Bytes                        Name
 -----                        ----
 1-9   (Length-Coded-Binary)  field_count
 1-9   (Length-Coded-Binary)  extra

The order of packets for a result set is: 
  (Result Set Header Packet)  the number of columns
  (Field Packets)             column descriptors
  (EOF Packet)                marker: end of Field Packets
  (Row Data Packets)          row contents
  (EOF Packet)                marker: end of Data Packets
*/
func (mc *mysqlConn) readResultSetHeaderPacket() (fieldCount int, e error) {
	data, e := mc.readPacket()
	if e != nil {
		errLog.Print(`packets:437 `, e)
		e = driver.ErrBadConn
		return
	}

	if data[0] == 255 {
		e = mc.handleErrorPacket(data)
		return
	} else if data[0] == 0 {
		e = mc.handleOkPacket(data)
		return
	}

	num, n, e := bytesToLengthCodedBinary(data)
	if e != nil || (n-len(data)) != 0 {
		e = errors.New("Malformed Packet")
		return
	}

	fieldCount = int(num)
	return
}

// Read Packets as Field Packets until EOF-Packet or an Error appears
func (mc *mysqlConn) readColumns(n int) (columns []mysqlField, e error) {
	var data []byte

	for {
		data, e = mc.readPacket()
		if e != nil {
			return
		}

		// EOF Packet
		if data[0] == 254 && len(data) == 5 {
			if len(columns) != n {
				e = fmt.Errorf("ColumnsCount mismatch n:%d len:%d", n, len(columns))
			}
			return
		}

		var pos, n int
		var name []byte
		//var catalog, database, table, orgTable, name, orgName []byte
		//var defaultVal uint64

		// Catalog
		//catalog, n, _, e = readLengthCodedBinary(data)
		n, e = readAndDropLengthCodedBinary(data)
		if e != nil {
			return
		}
		pos += n

		// Database [len coded string]
		//database, n, _, e = readLengthCodedBinary(data[pos:])
		n, e = readAndDropLengthCodedBinary(data[pos:])
		if e != nil {
			return
		}
		pos += n

		// Table [len coded string]
		//table, n, _, e = readLengthCodedBinary(data[pos:])
		n, e = readAndDropLengthCodedBinary(data[pos:])
		if e != nil {
			return
		}
		pos += n

		// Original table [len coded string]
		//orgTable, n, _, e = readLengthCodedBinary(data[pos:])
		n, e = readAndDropLengthCodedBinary(data[pos:])
		if e != nil {
			return
		}
		pos += n

		// Name [len coded string]
		name, n, _, e = readLengthCodedBinary(data[pos:])
		if e != nil {
			return
		}
		pos += n

		// Original name [len coded string]
		//orgName, n, _, e = readLengthCodedBinary(data[pos:])
		n, e = readAndDropLengthCodedBinary(data[pos:])
		if e != nil {
			return
		}
		pos += n

		// Filler
		pos++

		// Charset [16 bit uint]
		//charsetNumber := bytesToUint16(data[pos : pos+2])
		pos += 2

		// Length [32 bit uint]
		//length := bytesToUint32(data[pos : pos+4])
		pos += 4

		// Field type [byte]
		fieldType := FieldType(data[pos])
		pos++

		// Flags [16 bit uint]
		flags := FieldFlag(bytesToUint16(data[pos : pos+2]))
		//pos += 2

		// Decimals [8 bit uint]
		//decimals := data[pos]
		//pos++

		// Default value [len coded binary]
		//if pos < len(data) {
		//	defaultVal, _, e = bytesToLengthCodedBinary(data[pos:])
		//}

		columns = append(columns, mysqlField{name: string(name), fieldType: fieldType, flags: flags})
	}

	return
}

// Read Packets as Field Packets until EOF-Packet or an Error appears
func (mc *mysqlConn) readRows(columnsCount int) (rows []*[][]byte, e error) {
	var data []byte
	var i, pos, n int
	var isNull bool

	for {
		data, e = mc.readPacket()
		if e != nil {
			return
		}

		// EOF Packet
		if data[0] == 254 && len(data) == 5 {
			return
		}

		// RowSet Packet
		row := make([][]byte, columnsCount)
		pos = 0
		for i = 0; i < columnsCount; i++ {
			// Read bytes and convert to string
			row[i], n, isNull, e = readLengthCodedBinary(data[pos:])
			if e != nil {
				return
			}

			// Append nil if field is NULL
			if isNull {
				row[i] = nil
			}
			pos += n
		}
		rows = append(rows, &row)
	}

	mc.affectedRows = uint64(len(rows))
	return
}

// Reads Packets Packets until EOF-Packet or an Error appears. Returns count of Packets read
func (mc *mysqlConn) readUntilEOF() (count uint64, e error) {
	var data []byte

	for {
		data, e = mc.readPacket()
		if e != nil {
			return
		}

		// EOF Packet
		if data[0] == 254 && len(data) == 5 {
			return
		}

		count++
	}
	return
}

/******************************************************************************
*                           Prepared Statements                               *
******************************************************************************/

/* Prepare Result Packets 
 Type Of Result Packet       Hexadecimal Value Of First Byte (field_count)
 ---------------------       ---------------------------------------------

 Prepare OK Packet           00
 Error Packet                ff

Prepare OK Packet 
 Bytes              Name
 -----              ----
 1                  0 - marker for OK packet
 4                  statement_handler_id
 2                  number of columns in result set
 2                  number of parameters in query
 1                  filler (always 0)
 2                  warning count

 It is made up of:

    a PREPARE_OK packet
    if "number of parameters" > 0
        (field packets) as in a Result Set Header Packet
        (EOF packet) 
    if "number of columns" > 0
        (field packets) as in a Result Set Header Packet
        (EOF packet) 

*/
func (stmt mysqlStmt) readPrepareResultPacket() (columnCount uint16, e error) {
	data, e := stmt.mc.readPacket()
	if e != nil {
		return
	}

	// Position
	pos := 0

	if data[pos] != 0 {
		e = stmt.mc.handleErrorPacket(data)
		return
	}
	pos++

	stmt.id = bytesToUint32(data[pos : pos+4])
	pos += 4

	// Column count [16 bit uint]
	columnCount = bytesToUint16(data[pos : pos+2])
	pos += 2

	// Param count [16 bit uint]
	stmt.paramCount = int(bytesToUint16(data[pos : pos+2]))
	pos += 2

	// Warning count [16 bit uint]
	// bytesToUint16(data[pos : pos+2])

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
	argsLen := len(*args)
	if argsLen < stmt.paramCount {
		return fmt.Errorf(
			"Not enough Arguments to call STMT_EXEC (Got: %d Has: %d",
			argsLen,
			stmt.paramCount)
	}

	// Reset packet-sequence
	stmt.mc.sequence = 0

	pktLen := 1 + 4 + 1 + 4 + (stmt.paramCount+7)/8 + 1 + argsLen*2
	paramValues := make([][]byte, 0, argsLen)
	paramTypes := make([]byte, 0, argsLen*2)
	bitMask := uint64(0)
	var i, valLen int
	var pv reflect.Value
	for i = 0; i < stmt.paramCount; i++ {
		// build nullBitMap	
		if (*args)[i] == nil {
			bitMask += 1 << uint(i)
		}

		// cache types and values
		switch (*args)[i].(type) {
		case nil:
			paramTypes = append(paramTypes, []byte{
				byte(FIELD_TYPE_NULL),
				0x0}...)
			continue

		case []byte:
			paramTypes = append(paramTypes, []byte{byte(FIELD_TYPE_STRING), 0x0}...)
			val := (*args)[i].([]byte)
			valLen = len(val)
			lcb := lengthCodedBinaryToBytes(uint64(valLen))
			pktLen += len(lcb) + valLen
			paramValues = append(paramValues, lcb)
			paramValues = append(paramValues, val)
			continue

		case time.Time:
			// Format to string for time+date Fields
			// Data is packed in case reflect.String below
			(*args)[i] = (*args)[i].(time.Time).Format(TIME_FORMAT)
		}

		pv = reflect.ValueOf((*args)[i])
		switch pv.Kind() {
		case reflect.Int64:
			paramTypes = append(paramTypes, []byte{byte(FIELD_TYPE_LONGLONG), 0x0}...)
			val := int64ToBytes(pv.Int())
			pktLen += len(val)
			paramValues = append(paramValues, val)
			continue

		case reflect.Float64:
			paramTypes = append(paramTypes, []byte{byte(FIELD_TYPE_DOUBLE), 0x0}...)
			val := float64ToBytes(pv.Float())
			pktLen += len(val)
			paramValues = append(paramValues, val)
			continue

		case reflect.Bool:
			paramTypes = append(paramTypes, []byte{byte(FIELD_TYPE_TINY), 0x0}...)
			val := pv.Bool()
			pktLen++
			if val {
				paramValues = append(paramValues, []byte{byte(1)})
			} else {
				paramValues = append(paramValues, []byte{byte(0)})
			}
			continue

		case reflect.String:
			paramTypes = append(paramTypes, []byte{byte(FIELD_TYPE_STRING), 0x0}...)
			val := []byte(pv.String())
			valLen = len(val)
			lcb := lengthCodedBinaryToBytes(uint64(valLen))
			pktLen += valLen + len(lcb)
			paramValues = append(paramValues, lcb)
			paramValues = append(paramValues, val)
			continue

		default:
			return fmt.Errorf("Invalid Value: %s", pv.Kind().String())
		}
	}

	data := make([]byte, 0, pktLen+4)

	// Add the packet header
	data = append(data, uint24ToBytes(uint32(pktLen))...)
	data = append(data, stmt.mc.sequence)

	// code [1 byte]
	data = append(data, byte(COM_STMT_EXECUTE))

	// statement_id [4 bytes]
	data = append(data, uint32ToBytes(stmt.id)...)

	// flags (0: CURSOR_TYPE_NO_CURSOR) [1 byte]
	data = append(data, byte(0))

	// iteration_count [4 bytes]
	data = append(data, uint32ToBytes(1)...)

	// append nullBitMap [(param_count+7)/8 bytes]
	if stmt.paramCount > 0 {
		// Convert bitMask to bytes
		nullBitMap := make([]byte, (stmt.paramCount+7)/8)
		for i = 0; i < len(nullBitMap); i++ {
			nullBitMap[i] = byte(bitMask >> uint(i*8))
		}

		data = append(data, nullBitMap...)
	}

	// newParameterBoundFlag 1 [1 byte]
	data = append(data, byte(1))

	// type of parameters [n*2 byte]
	data = append(data, paramTypes...)

	// values for the parameters [n byte]
	for _, paramValue := range paramValues {
		data = append(data, paramValue...)
	}

	return stmt.mc.writePacket(&data)
}

func (mc *mysqlConn) readBinaryRows(rc *rowsContent) (e error) {
	var data, nullBitMap []byte
	var i, pos, n int
	var unsigned, isNull bool
	columnsCount := len(rc.columns)

	for {
		data, e = mc.readPacket()
		if e != nil {
			return
		}

		pos = 0

		// EOF Packet
		if data[pos] == 254 && len(data) == 5 {
			return
		}

		pos++

		// BinaryRowSet Packet
		row := make([][]byte, columnsCount)

		nullBitMap = data[pos : pos+(columnsCount+7+2)/8]
		pos += (columnsCount + 7 + 2) / 8

		for i = 0; i < columnsCount; i++ {
			// Field is NULL
			if (nullBitMap[(i+2)/8] >> uint((i+2)%8) & 1) == 1 {
				row[i] = nil
				continue
			}

			unsigned = rc.columns[i].flags&FLAG_UNSIGNED != 0

			// Convert to byte-coded string
			switch rc.columns[i].fieldType {
			case FIELD_TYPE_NULL:
				row[i] = nil

			// Numeric Typs
			case FIELD_TYPE_TINY:
				if unsigned {
					row[i] = uintToByteStr(uint64(byteToUint8(data[pos])))
				} else {
					row[i] = intToByteStr(int64(int8(byteToUint8(data[pos]))))
				}
				pos++

			case FIELD_TYPE_SHORT, FIELD_TYPE_YEAR:
				if unsigned {
					row[i] = uintToByteStr(uint64(bytesToUint16(data[pos : pos+2])))
				} else {
					row[i] = intToByteStr(int64(int16(bytesToUint16(data[pos : pos+2]))))
				}
				pos += 2

			case FIELD_TYPE_INT24, FIELD_TYPE_LONG:
				if unsigned {
					row[i] = uintToByteStr(uint64(bytesToUint32(data[pos : pos+4])))
				} else {
					row[i] = intToByteStr(int64(int32(bytesToUint32(data[pos : pos+4]))))
				}
				pos += 4

			case FIELD_TYPE_LONGLONG:
				if unsigned {
					row[i] = uintToByteStr(bytesToUint64(data[pos : pos+8]))
				} else {
					row[i] = intToByteStr(int64(bytesToUint64(data[pos : pos+8])))
				}
				pos += 8

			case FIELD_TYPE_FLOAT:
				row[i] = float32ToByteStr(bytesToFloat32(data[pos : pos+4]))
				pos += 4

			case FIELD_TYPE_DOUBLE:
				row[i] = float64ToByteStr(bytesToFloat64(data[pos : pos+8]))
				pos += 8

			case FIELD_TYPE_DECIMAL, FIELD_TYPE_NEWDECIMAL:
				row[i], n, isNull, e = readLengthCodedBinary(data[pos:])
				if e != nil {
					return
				}

				if isNull && rc.columns[i].flags&FLAG_NOT_NULL == 0 {
					row[i] = nil
				}
				pos += n

			// Length coded Binary Strings
			case FIELD_TYPE_VARCHAR, FIELD_TYPE_BIT, FIELD_TYPE_ENUM,
				FIELD_TYPE_SET, FIELD_TYPE_TINY_BLOB, FIELD_TYPE_MEDIUM_BLOB,
				FIELD_TYPE_LONG_BLOB, FIELD_TYPE_BLOB, FIELD_TYPE_VAR_STRING,
				FIELD_TYPE_STRING, FIELD_TYPE_GEOMETRY:
				row[i], n, isNull, e = readLengthCodedBinary(data[pos:])
				if e != nil {
					return
				}

				if isNull && rc.columns[i].flags&FLAG_NOT_NULL == 0 {
					row[i] = nil
				}
				pos += n

			// Date YYYY-MM-DD
			case FIELD_TYPE_DATE, FIELD_TYPE_NEWDATE:
				var num uint64
				num, n, e = bytesToLengthCodedBinary(data[pos:])
				if e != nil {
					return
				}
				pos += n

				if num == 0 {
					row[i] = []byte("0000-00-00")
				} else {
					row[i] = []byte(fmt.Sprintf("%04d-%02d-%02d",
						bytesToUint16(data[pos:pos+2]),
						data[pos+2],
						data[pos+3]))
				}
				pos += int(num)

			// Time HH:MM:SS
			case FIELD_TYPE_TIME:
				var num uint64
				num, n, e = bytesToLengthCodedBinary(data[pos:])
				if e != nil {
					return
				}

				if num == 0 {
					row[i] = []byte("00:00:00")
				} else {
					row[i] = []byte(fmt.Sprintf("%02d:%02d:%02d",
						data[pos+6],
						data[pos+7],
						data[pos+8]))
				}
				pos += n + int(num)

			// Timestamp YYYY-MM-DD HH:MM:SS
			case FIELD_TYPE_TIMESTAMP, FIELD_TYPE_DATETIME:
				var num uint64
				num, n, e = bytesToLengthCodedBinary(data[pos:])
				if e != nil {
					return
				}
				pos += n

				switch num {
				case 0:
					row[i] = []byte("0000-00-00 00:00:00")
				case 4:
					row[i] = []byte(fmt.Sprintf("%04d-%02d-%02d 00:00:00",
						bytesToUint16(data[pos:pos+2]),
						data[pos+2],
						data[pos+3]))
				default:
					if num < 7 {
						return fmt.Errorf("Invalid datetime-packet length %d", num)
					}
					row[i] = []byte(fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d",
						bytesToUint16(data[pos:pos+2]),
						data[pos+2],
						data[pos+3],
						data[pos+4],
						data[pos+5],
						data[pos+6]))
				}
				pos += int(num)

			// Please report if this happens!
			default:
				return fmt.Errorf("Unknown FieldType %d", rc.columns[i].fieldType)
			}
		}
		rc.rows = append(rc.rows, &row)
	}

	mc.affectedRows = uint64(len(rc.rows))
	return
}

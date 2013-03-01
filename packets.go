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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"reflect"
	"time"
)

// Packets documentation:
// http://dev.mysql.com/doc/internals/en/client-server-protocol.html

// Read packet to buffer 'data'
func (mc *mysqlConn) readPacket() (data []byte, err error) {
	// Read header
	data = make([]byte, 4)
	var n, add int
	for err == nil && n < 4 {
		add, err = mc.bufReader.Read(data[n:])
		n += add
	}

	// Packet Length
	var pktLen uint32
	pktLen |= uint32(data[0])
	pktLen |= uint32(data[1]) << 8
	pktLen |= uint32(data[2]) << 16

	if pktLen == 0 {
		return nil, err
	}

	// Check Packet Sync
	if data[3] != mc.sequence {
		if data[3] > mc.sequence {
			err = errors.New("Commands out of sync. Did you run multiple statements at once?")
		} else {
			err = errors.New("Commands out of sync; you can't run this command now")
		}
		return nil, err
	}
	mc.sequence++

	// Read rest of packet
	data = make([]byte, pktLen)
	n = 0
	for err == nil && n < int(pktLen) {
		add, err = mc.bufReader.Read(data[n:])
		n += add
	}
	if err != nil || n < int(pktLen) {
		if err == nil {
			err = fmt.Errorf("Length of read data (%d) does not match body length (%d)", n, pktLen)
		}
		errLog.Print(err)
		return nil, driver.ErrBadConn
	}

	return data, err
}

func (mc *mysqlConn) writePacket(data *[]byte) error {
	// Write packet
	n, err := mc.netConn.Write(*data)
	if err != nil || n != len(*data) {
		if err == nil {
			err = errors.New("Length of send data does not match packet length")
		}
		errLog.Print(err)
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
func (mc *mysqlConn) readInitPacket() (err error) {
	data, err := mc.readPacket()
	if err != nil {
		return
	}

	mc.server = new(serverSettings)

	// Position
	pos := 0

	// Protocol version [8 bit uint]
	mc.server.protocol = data[pos]
	if mc.server.protocol < MIN_PROTOCOL_VERSION {
		err = fmt.Errorf(
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
	mc.server.threadID = binary.LittleEndian.Uint32(data[pos : pos+4])
	pos += 4

	// First part of scramble buffer [8 bytes]
	mc.server.scrambleBuff = make([]byte, 8)
	mc.server.scrambleBuff = data[pos : pos+8]
	pos += 9

	// Server capabilities [16 bit uint]
	mc.server.flags = ClientFlag(binary.LittleEndian.Uint16(data[pos : pos+2]))
	if mc.server.flags&CLIENT_PROTOCOL_41 == 0 {
		err = errors.New("MySQL-Server does not support required Protocol 41+")
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
func (mc *mysqlConn) writeAuthPacket() error {
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
func (mc *mysqlConn) writeCommandPacket(command commandType, args ...interface{}) error {
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
func (mc *mysqlConn) readResultOK() error {
	data, err := mc.readPacket()
	if err != nil {
		return err
	}

	switch data[0] {
	// OK
	case 0:
		return mc.handleOkPacket(data)
	// EOF, someone is using old_passwords
	case 254:
		return errors.New("It seems like you are using old_passwords, which is unsupported. See https://github.com/Go-SQL-Driver/MySQL/wiki/old_passwords")
	// ERROR
	case 255:
		return mc.handleErrorPacket(data)
	}

	return errors.New("Invalid Result Packet-Type")
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
func (mc *mysqlConn) handleErrorPacket(data []byte) error {
	if data[0] != 255 {
		return errors.New("Wrong Packet-Type: Not an Error-Packet")
	}

	pos := 1

	// Error Number [16 bit uint]
	errno := binary.LittleEndian.Uint16(data[pos : pos+2])
	pos += 2

	// SQL State [# + 5bytes string]
	//sqlstate := string(data[pos : pos+6])
	pos += 6

	// Error Message [string]
	message := string(data[pos:])

	return fmt.Errorf("Error %d: %s", errno, message)
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
func (mc *mysqlConn) handleOkPacket(data []byte) (err error) {
	if data[0] != 0 {
		err = errors.New("Wrong Packet-Type: Not an OK-Packet")
		return
	}

	// Position
	pos := 1

	var n int

	// Affected rows [Length Coded Binary]
	mc.affectedRows, _, n, err = bytesToLengthEncodedInteger(data[pos:])
	if err != nil {
		return
	}
	pos += n

	// Insert id [Length Coded Binary]
	mc.insertId, _, n, err = bytesToLengthEncodedInteger(data[pos:])
	if err != nil {
		return
	}

	// Skip remaining data

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
func (mc *mysqlConn) readResultSetHeaderPacket() (fieldCount int, err error) {
	data, err := mc.readPacket()
	if err != nil {
		errLog.Print(err)
		err = driver.ErrBadConn
		return
	}

	if data[0] == 255 {
		err = mc.handleErrorPacket(data)
		return
	} else if data[0] == 0 {
		err = mc.handleOkPacket(data)
		return
	}

	num, _, n, err := bytesToLengthEncodedInteger(data)
	if err != nil || (n-len(data)) != 0 {
		err = errors.New("Malformed Packet")
		return
	}

	fieldCount = int(num)
	return
}

// Read Packets as Field Packets until EOF-Packet or an Error appears
func (mc *mysqlConn) readColumns(count int) (columns []mysqlField, err error) {
	var data []byte
	var pos, n int
	var name []byte

	for {
		data, err = mc.readPacket()
		if err != nil {
			return
		}

		// EOF Packet
		if data[0] == 254 && len(data) == 5 {
			if len(columns) != count {
				err = fmt.Errorf("ColumnsCount mismatch n:%d len:%d", count, len(columns))
			}
			return
		}

		pos = 0

		// Catalog
		n, err = readAndDropLengthEnodedString(data)
		if err != nil {
			return
		}
		pos += n

		// Database [len coded string]
		n, err = readAndDropLengthEnodedString(data[pos:])
		if err != nil {
			return
		}
		pos += n

		// Table [len coded string]
		n, err = readAndDropLengthEnodedString(data[pos:])
		if err != nil {
			return
		}
		pos += n

		// Original table [len coded string]
		n, err = readAndDropLengthEnodedString(data[pos:])
		if err != nil {
			return
		}
		pos += n

		// Name [len coded string]
		name, _, n, err = readLengthEnodedString(data[pos:])
		if err != nil {
			return
		}
		pos += n

		// Original name [len coded string]
		n, err = readAndDropLengthEnodedString(data[pos:])
		if err != nil {
			return
		}
		pos += n

		// Filler
		pos++

		// Charset [16 bit uint]
		pos += 2

		// Length [32 bit uint]
		pos += 4

		// Field type [byte]
		fieldType := FieldType(data[pos])
		pos++

		// Flags [16 bit uint]
		flags := FieldFlag(binary.LittleEndian.Uint16(data[pos : pos+2]))
		//pos += 2

		// Decimals [8 bit uint]
		//pos++

		// Default value [len coded binary]
		//if pos < len(data) {
		//	defaultVal, _, err = bytesToLengthCodedBinary(data[pos:])
		//}

		columns = append(columns, mysqlField{name: string(name), fieldType: fieldType, flags: flags})
	}

	return
}

// Read Packets as Field Packets until EOF-Packet or an Error appears
func (rows *mysqlRows) readRow(dest *[]driver.Value) (err error) {
	data, err := rows.mc.readPacket()
	if err != nil {
		return
	}

	// EOF Packet
	if data[0] == 254 && len(data) == 5 {
		return io.EOF
	}

	// RowSet Packet
	var n int
	columnsCount := len(*dest)
	pos := 0

	for i := 0; i < columnsCount; i++ {
		// Read bytes and convert to string
		(*dest)[i], _, n, err = readLengthEnodedString(data[pos:])
		if err != nil {
			return
		}
		pos += n
	}

	return
}

// Reads Packets until EOF-Packet or an Error appears. Returns count of Packets read
func (mc *mysqlConn) readUntilEOF() (count uint64, err error) {
	var data []byte

	for {
		data, err = mc.readPacket()
		if err != nil {
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
func (stmt *mysqlStmt) readPrepareResultPacket() (columnCount uint16, err error) {
	data, err := stmt.mc.readPacket()
	if err != nil {
		return
	}

	// Position
	pos := 0

	if data[pos] != 0 {
		err = stmt.mc.handleErrorPacket(data)
		return
	}
	pos++

	stmt.id = binary.LittleEndian.Uint32(data[pos : pos+4])
	pos += 4

	// Column count [16 bit uint]
	columnCount = binary.LittleEndian.Uint16(data[pos : pos+2])
	pos += 2

	// Param count [16 bit uint]
	stmt.paramCount = int(binary.LittleEndian.Uint16(data[pos : pos+2]))
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
func (stmt *mysqlStmt) buildExecutePacket(args *[]driver.Value) error {
	argsLen := len(*args)
	if argsLen < stmt.paramCount {
		return fmt.Errorf(
			"Not enough Arguments to call STMT_EXEC (Got: %d Has: %d",
			argsLen,
			stmt.paramCount)
	}

	// Reset packet-sequence
	stmt.mc.sequence = 0

	pktLen := 1 + 4 + 1 + 4 + ((stmt.paramCount + 7) >> 3) + 1 + (argsLen << 1)
	paramValues := make([][]byte, 0, argsLen)
	paramTypes := make([]byte, 0, (argsLen << 1))
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
			lcb := lengthEncodedIntegerToBytes(uint64(valLen))
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
			lcb := lengthEncodedIntegerToBytes(uint64(valLen))
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

// http://dev.mysql.com/doc/internals/en/prepared-statements.html#packet-ProtocolBinary::ResultsetRow
func (rc *mysqlRows) readBinaryRow(dest *[]driver.Value) (err error) {
	data, err := rc.mc.readPacket()
	if err != nil {
		return
	}

	pos := 0

	// EOF Packet
	if data[pos] == 254 && len(data) == 5 {
		return io.EOF
	}
	pos++

	// BinaryRowSet Packet
	columnsCount := len(*dest)

	nullBitMap := data[pos : pos+(columnsCount+7+2)>>3]
	pos += (columnsCount + 7 + 2) >> 3

	var n int
	var unsigned bool
	for i := 0; i < columnsCount; i++ {
		// Field is NULL
		if (nullBitMap[(i+2)>>3] >> uint((i+2)&7) & 1) == 1 {
			(*dest)[i] = nil
			continue
		}

		unsigned = rc.columns[i].flags&FLAG_UNSIGNED != 0

		// Convert to byte-coded string
		switch rc.columns[i].fieldType {
		case FIELD_TYPE_NULL:
			(*dest)[i] = nil

		// Numeric Typs
		case FIELD_TYPE_TINY:
			if unsigned {
				(*dest)[i] = uint64(data[pos])
			} else {
				(*dest)[i] = int64(int8(data[pos]))
			}
			pos++

		case FIELD_TYPE_SHORT, FIELD_TYPE_YEAR:
			if unsigned {
				(*dest)[i] = uint64(binary.LittleEndian.Uint16(data[pos : pos+2]))
			} else {
				(*dest)[i] = int64(int16(binary.LittleEndian.Uint16(data[pos : pos+2])))
			}
			pos += 2

		case FIELD_TYPE_INT24, FIELD_TYPE_LONG:
			if unsigned {
				(*dest)[i] = uint64(binary.LittleEndian.Uint32(data[pos : pos+4]))
			} else {
				(*dest)[i] = int64(int32(binary.LittleEndian.Uint32(data[pos : pos+4])))
			}
			pos += 4

		case FIELD_TYPE_LONGLONG:
			if unsigned {
				(*dest)[i] = binary.LittleEndian.Uint64(data[pos : pos+8])
			} else {
				(*dest)[i] = int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
			}
			pos += 8

		case FIELD_TYPE_FLOAT:
			(*dest)[i] = bytesToFloat32(data[pos : pos+4])
			pos += 4

		case FIELD_TYPE_DOUBLE:
			(*dest)[i] = bytesToFloat64(data[pos : pos+8])
			pos += 8

		// Length coded Binary Strings
		case FIELD_TYPE_DECIMAL, FIELD_TYPE_NEWDECIMAL, FIELD_TYPE_VARCHAR,
			FIELD_TYPE_BIT, FIELD_TYPE_ENUM, FIELD_TYPE_SET,
			FIELD_TYPE_TINY_BLOB, FIELD_TYPE_MEDIUM_BLOB, FIELD_TYPE_LONG_BLOB,
			FIELD_TYPE_BLOB, FIELD_TYPE_VAR_STRING, FIELD_TYPE_STRING,
			FIELD_TYPE_GEOMETRY:
			(*dest)[i], _, n, err = readLengthEnodedString(data[pos:])
			if err != nil {
				return
			}
			pos += n

		// Date YYYY-MM-DD
		case FIELD_TYPE_DATE, FIELD_TYPE_NEWDATE:
			var num uint64
			// TODO(js): allow nil values
			num, _, n, err = bytesToLengthEncodedInteger(data[pos:])
			if err != nil {
				return
			}
			pos += n

			if num == 0 {
				(*dest)[i] = []byte("0000-00-00")
			} else {
				(*dest)[i] = []byte(fmt.Sprintf("%04d-%02d-%02d",
					binary.LittleEndian.Uint16(data[pos:pos+2]),
					data[pos+2],
					data[pos+3]))
			}
			pos += int(num)

		// Time HH:MM:SS
		case FIELD_TYPE_TIME:
			var num uint64
			// TODO(js): allow nil values
			num, _, n, err = bytesToLengthEncodedInteger(data[pos:])
			if err != nil {
				return
			}

			if num == 0 {
				(*dest)[i] = []byte("00:00:00")
			} else {
				(*dest)[i] = []byte(fmt.Sprintf("%02d:%02d:%02d",
					data[pos+6],
					data[pos+7],
					data[pos+8]))
			}
			pos += n + int(num)

		// Timestamp YYYY-MM-DD HH:MM:SS
		case FIELD_TYPE_TIMESTAMP, FIELD_TYPE_DATETIME:
			var num uint64
			// TODO(js): allow nil values
			num, _, n, err = bytesToLengthEncodedInteger(data[pos:])
			if err != nil {
				return
			}
			pos += n

			switch num {
			case 0:
				(*dest)[i] = []byte("0000-00-00 00:00:00")
			case 4:
				(*dest)[i] = []byte(fmt.Sprintf("%04d-%02d-%02d 00:00:00",
					binary.LittleEndian.Uint16(data[pos:pos+2]),
					data[pos+2],
					data[pos+3]))
			default:
				if num < 7 {
					return fmt.Errorf("Invalid datetime-packet length %d", num)
				}
				(*dest)[i] = []byte(fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d",
					binary.LittleEndian.Uint16(data[pos:pos+2]),
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

	return
}

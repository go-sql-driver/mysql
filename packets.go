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
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"time"
)

// Packets documentation:
// http://dev.mysql.com/doc/internals/en/client-server-protocol.html

// Read packet to buffer 'data'
func (mc *mysqlConn) readPacket() (data []byte, err error) {
	// Read packet header
	data = make([]byte, 4)
	err = mc.buf.read(data)
	if err != nil {
		errLog.Print(err.Error())
		return nil, driver.ErrBadConn
	}

	// Packet Length [24 bit]
	pktLen := uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16

	if pktLen < 1 {
		errLog.Print(errMalformPkt.Error())
		return nil, driver.ErrBadConn
	}

	// Check Packet Sync [8 bit]
	if data[3] != mc.sequence {
		if data[3] > mc.sequence {
			return nil, errPktSyncMul
		} else {
			return nil, errPktSync
		}
	}
	mc.sequence++

	// Read packet body [pktLen bytes]
	data = make([]byte, pktLen)
	err = mc.buf.read(data)
	if err == nil {
		if pktLen < maxPacketSize {
			return data, nil
		}

		// More data
		var data2 []byte
		data2, err = mc.readPacket()
		if err == nil {
			return append(data, data2...), nil
		}
	}
	errLog.Print(err.Error())
	return nil, driver.ErrBadConn
}

// Write packet buffer 'data'
// The packet header must be already included
func (mc *mysqlConn) writePacket(data []byte) error {
	if len(data)-4 <= mc.maxWriteSize { // Can send data at once
		// Write packet
		n, err := mc.netConn.Write(data)
		if err == nil && n == len(data) {
			mc.sequence++
			return nil
		}

		// Handle error
		if err == nil { // n != len(data)
			errLog.Print(errMalformPkt.Error())
		} else {
			errLog.Print(err.Error())
		}
		return driver.ErrBadConn
	}

	// Must split packet
	return mc.splitPacket(data)
}

func (mc *mysqlConn) splitPacket(data []byte) (err error) {
	pktLen := len(data) - 4

	if pktLen > mc.maxPacketAllowed {
		return errPktTooLarge
	}

	for pktLen >= maxPacketSize {
		data[0] = 0xff
		data[1] = 0xff
		data[2] = 0xff
		data[3] = mc.sequence

		// Write packet
		n, err := mc.netConn.Write(data[:4+maxPacketSize])
		if err == nil && n == 4+maxPacketSize {
			mc.sequence++
			data = data[maxPacketSize:]
			pktLen -= maxPacketSize
			continue
		}

		// Handle error
		if err == nil { // n != len(data)
			errLog.Print(errMalformPkt.Error())
		} else {
			errLog.Print(err.Error())
		}
		return driver.ErrBadConn
	}

	data[0] = byte(pktLen)
	data[1] = byte(pktLen >> 8)
	data[2] = byte(pktLen >> 16)
	data[3] = mc.sequence
	return mc.writePacket(data)
}

/******************************************************************************
*                           Initialisation Process                            *
******************************************************************************/

// Handshake Initialization Packet
// http://dev.mysql.com/doc/internals/en/connection-phase.html#packet-Protocol::Handshake
func (mc *mysqlConn) readInitPacket() (err error) {
	data, err := mc.readPacket()
	if err != nil {
		return
	}

	if data[0] == iERR {
		return mc.handleErrorPacket(data)
	}

	// protocol version [1 byte]
	if data[0] < minProtocolVersion {
		err = fmt.Errorf(
			"Unsupported MySQL Protocol Version %d. Protocol Version %d or higher is required",
			data[0],
			minProtocolVersion)
	}

	// server version [null terminated string]
	// connection id [4 bytes]
	pos := 1 + bytes.IndexByte(data[1:], 0x00) + 1 + 4

	// first part of the password cipher [8 bytes]
	mc.cipher = append(mc.cipher, data[pos:pos+8]...)

	// (filler) always 0x00 [1 byte]
	pos += 8 + 1

	// capability flags (lower 2 bytes) [2 bytes]
	mc.flags = clientFlag(binary.LittleEndian.Uint16(data[pos : pos+2]))
	if mc.flags&clientProtocol41 == 0 {
		err = errors.New("MySQL-Server does not support required Protocol 41+")
	}
	pos += 2

	if len(data) > pos {
		// character set [1 byte]
		mc.charset = data[pos]

		// status flags [2 bytes]
		// capability flags (upper 2 bytes) [2 bytes]
		// length of auth-plugin-data [1 byte]
		// reserved (all [00]) [10 bytes]
		pos += 1 + 2 + 2 + 1 + 10

		// second part of the password cipher [12? bytes]
		// The documentation is ambiguous about the length.
		// The official Python library uses the fixed length 12
		// which is not documented but seems to work.
		mc.cipher = append(mc.cipher, data[pos:pos+12]...)

		if data[len(data)-1] == 0 {
			return
		}
		return errMalformPkt
	}

	return
}

// Client Authentication Packet
// http://dev.mysql.com/doc/internals/en/connection-phase.html#packet-Protocol::HandshakeResponse
func (mc *mysqlConn) writeAuthPacket() error {
	// Adjust client flags based on server support
	clientFlags := uint32(
		clientProtocol41 |
			clientSecureConn |
			clientLongPassword |
			clientTransactions |
			clientLocalFiles,
	)
	if mc.flags&clientLongFlag > 0 {
		clientFlags |= uint32(clientLongFlag)
	}

	// User Password
	scrambleBuff := scramblePassword(mc.cipher, []byte(mc.cfg.passwd))
	mc.cipher = nil

	pktLen := 4 + 4 + 1 + 23 + len(mc.cfg.user) + 1 + 1 + len(scrambleBuff)

	// To specify a db name
	if len(mc.cfg.dbname) > 0 {
		clientFlags |= uint32(clientConnectWithDB)
		pktLen += len(mc.cfg.dbname) + 1
	}

	// Calculate packet length and make buffer with that size
	data := make([]byte, pktLen+4)

	// Add the packet header  [24bit length + 1 byte sequence]
	data[0] = byte(pktLen)
	data[1] = byte(pktLen >> 8)
	data[2] = byte(pktLen >> 16)
	data[3] = mc.sequence

	// ClientFlags [32 bit]
	data[4] = byte(clientFlags)
	data[5] = byte(clientFlags >> 8)
	data[6] = byte(clientFlags >> 16)
	data[7] = byte(clientFlags >> 24)

	// MaxPacketSize [32 bit] (none)
	//data[8] = 0x00
	//data[9] = 0x00
	//data[10] = 0x00
	//data[11] = 0x00

	// Charset [1 byte]
	data[12] = mc.charset

	// Filler [23 bytes] (all 0x00)
	pos := 13 + 23

	// User [null terminated string]
	if len(mc.cfg.user) > 0 {
		pos += copy(data[pos:], mc.cfg.user)
	}
	//data[pos] = 0x00
	pos++

	// ScrambleBuffer [length encoded integer]
	data[pos] = byte(len(scrambleBuff))
	pos += 1 + copy(data[pos+1:], scrambleBuff)

	// Databasename [null terminated string]
	if len(mc.cfg.dbname) > 0 {
		pos += copy(data[pos:], mc.cfg.dbname)
		//data[pos] = 0x00
	}

	// Send Auth packet
	return mc.writePacket(data)
}

/******************************************************************************
*                             Command Packets                                 *
******************************************************************************/

func (mc *mysqlConn) writeCommandPacket(command byte) error {
	// Reset Packet Sequence
	mc.sequence = 0

	// Send CMD packet
	return mc.writePacket([]byte{
		// Add the packet header [24bit length + 1 byte sequence]
		0x05, // 5 bytes long
		0x00,
		0x00,
		0x00, // mc.sequence

		// Add command byte
		command,
	})
}

func (mc *mysqlConn) writeCommandPacketStr(command byte, arg string) error {
	// Reset Packet Sequence
	mc.sequence = 0

	pktLen := 1 + len(arg)
	data := make([]byte, pktLen+4)

	// Add the packet header [24bit length + 1 byte sequence]
	data[0] = byte(pktLen)
	data[1] = byte(pktLen >> 8)
	data[2] = byte(pktLen >> 16)
	//data[3] = mc.sequence

	// Add command byte
	data[4] = command

	// Add arg
	copy(data[5:], arg)

	// Send CMD packet
	return mc.writePacket(data)
}

func (mc *mysqlConn) writeCommandPacketUint32(command byte, arg uint32) error {
	// Reset Packet Sequence
	mc.sequence = 0

	// Send CMD packet
	return mc.writePacket([]byte{
		// Add the packet header [24bit length + 1 byte sequence]
		0x05, // 5 bytes long
		0x00,
		0x00,
		0x00, // mc.sequence

		// Add command byte
		command,

		// Add arg [32 bit]
		byte(arg),
		byte(arg >> 8),
		byte(arg >> 16),
		byte(arg >> 24),
	})
}

/******************************************************************************
*                              Result Packets                                 *
******************************************************************************/

// Returns error if Packet is not an 'Result OK'-Packet
func (mc *mysqlConn) readResultOK() error {
	data, err := mc.readPacket()
	if err == nil {
		// packet indicator
		switch data[0] {

		case iOK:
			mc.handleOkPacket(data)
			return nil

		case iEOF: // someone is using old_passwords
			return errOldPassword

		default: // Error otherwise
			return mc.handleErrorPacket(data)
		}
	}
	return err
}

// Result Set Header Packet
// http://dev.mysql.com/doc/internals/en/text-protocol.html#packet-ProtocolText::Resultset
func (mc *mysqlConn) readResultSetHeaderPacket() (int, error) {
	data, err := mc.readPacket()
	if err == nil {
		switch data[0] {

		case iOK:
			mc.handleOkPacket(data)
			return 0, nil

		case iERR:
			return 0, mc.handleErrorPacket(data)

		case iLocalInFile:
			return 0, mc.handleInFileRequest(string(data[1:]))
		}

		// column count
		num, _, n := readLengthEncodedInteger(data)
		if n-len(data) == 0 {
			return int(num), nil
		}

		return 0, errMalformPkt
	}
	return 0, err
}

// Error Packet
// http://dev.mysql.com/doc/internals/en/overview.html#packet-ERR_Packet
func (mc *mysqlConn) handleErrorPacket(data []byte) error {
	if data[0] != iERR {
		return errMalformPkt
	}

	// 0xff [1 byte]

	// Error Number [16 bit uint]
	errno := binary.LittleEndian.Uint16(data[1:3])

	pos := 3

	// SQL State [optional: # + 5bytes string]
	//sqlstate := string(data[pos : pos+6])
	if data[pos] == 0x23 {
		pos = 9
	}

	// Error Message [string]
	return fmt.Errorf("Error %d: %s", errno, string(data[pos:]))
}

// Ok Packet
// http://dev.mysql.com/doc/internals/en/overview.html#packet-OK_Packet
func (mc *mysqlConn) handleOkPacket(data []byte) {
	var n int

	// 0x00 [1 byte]

	// Affected rows [Length Coded Binary]
	mc.affectedRows, _, n = readLengthEncodedInteger(data[1:])

	// Insert id [Length Coded Binary]
	mc.insertId, _, _ = readLengthEncodedInteger(data[1+n:])

	// server_status [2 bytes]
	// warning count [2 bytes]
	// message [until end of packet]
}

// Read Packets as Field Packets until EOF-Packet or an Error appears
// http://dev.mysql.com/doc/internals/en/text-protocol.html#packet-Protocol::ColumnDefinition41
func (mc *mysqlConn) readColumns(count int) (columns []mysqlField, err error) {
	var data []byte
	var i, pos, n int
	var name []byte

	columns = make([]mysqlField, count)

	for {
		data, err = mc.readPacket()
		if err != nil {
			return
		}

		// EOF Packet
		if data[0] == iEOF && len(data) == 5 {
			if i != count {
				err = fmt.Errorf("ColumnsCount mismatch n:%d len:%d", count, len(columns))
			}
			return
		}

		// Catalog
		pos, err = skipLengthEnodedString(data)
		if err != nil {
			return
		}

		// Database [len coded string]
		n, err = skipLengthEnodedString(data[pos:])
		if err != nil {
			return
		}
		pos += n

		// Table [len coded string]
		n, err = skipLengthEnodedString(data[pos:])
		if err != nil {
			return
		}
		pos += n

		// Original table [len coded string]
		n, err = skipLengthEnodedString(data[pos:])
		if err != nil {
			return
		}
		pos += n

		// Name [len coded string]
		name, _, n, err = readLengthEnodedString(data[pos:])
		if err != nil {
			return
		}
		columns[i].name = string(name)
		pos += n

		// Original name [len coded string]
		n, err = skipLengthEnodedString(data[pos:])
		if err != nil {
			return
		}

		// Filler [1 byte]
		// Charset [16 bit uint]
		// Length [32 bit uint]
		pos += n + 1 + 2 + 4

		// Field type [byte]
		columns[i].fieldType = data[pos]
		pos++

		// Flags [16 bit uint]
		columns[i].flags = fieldFlag(binary.LittleEndian.Uint16(data[pos : pos+2]))
		//pos += 2

		// Decimals [8 bit uint]
		//pos++

		// Default value [len coded binary]
		//if pos < len(data) {
		//	defaultVal, _, err = bytesToLengthCodedBinary(data[pos:])
		//}

		i++
	}

	return
}

// Read Packets as Field Packets until EOF-Packet or an Error appears
// http://dev.mysql.com/doc/internals/en/text-protocol.html#packet-ProtocolText::ResultsetRow
func (rows *mysqlRows) readRow(dest []driver.Value) (err error) {
	data, err := rows.mc.readPacket()
	if err != nil {
		return
	}

	// EOF Packet
	if data[0] == iEOF && len(data) == 5 {
		return io.EOF
	}

	// RowSet Packet
	var n int
	var isNull bool
	pos := 0

	for i := range dest {
		// Read bytes and convert to string
		dest[i], isNull, n, err = readLengthEnodedString(data[pos:])
		pos += n
		if err == nil {
			if !isNull {
				continue
			} else {
				dest[i] = nil
				continue
			}
		}
		return // err
	}

	return
}

// Reads Packets until EOF-Packet or an Error appears. Returns count of Packets read
func (mc *mysqlConn) readUntilEOF() (err error) {
	var data []byte

	for {
		data, err = mc.readPacket()

		// No Err and no EOF Packet
		if err == nil && (data[0] != iEOF || len(data) != 5) {
			continue
		}
		return // Err or EOF
	}
	return
}

/******************************************************************************
*                           Prepared Statements                               *
******************************************************************************/

// Prepare Result Packets
// http://dev.mysql.com/doc/internals/en/prepared-statements.html#com-stmt-prepare-response
func (stmt *mysqlStmt) readPrepareResultPacket() (columnCount uint16, err error) {
	data, err := stmt.mc.readPacket()
	if err == nil {
		// Position
		pos := 0

		// packet indicator [1 byte]
		if data[pos] != iOK {
			err = stmt.mc.handleErrorPacket(data)
			return
		}
		pos++

		// statement id [4 bytes]
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
	}
	return
}

// http://dev.mysql.com/doc/internals/en/prepared-statements.html#com-stmt-send-long-data
func (stmt *mysqlStmt) writeCommandLongData(paramID int, arg []byte) (err error) {
	maxLen := stmt.mc.maxPacketAllowed - 1
	pktLen := maxLen
	argLen := len(arg)
	data := make([]byte, 4+1+4+2+argLen)
	copy(data[4+1+4+2:], arg)

	for argLen > 0 {
		if 1+4+2+argLen < maxLen {
			pktLen = 1 + 4 + 2 + argLen
		}

		// Add the packet header [24bit length + 1 byte sequence]
		data[0] = byte(pktLen)
		data[1] = byte(pktLen >> 8)
		data[2] = byte(pktLen >> 16)
		data[3] = 0x00 // mc.sequence

		// Add command byte [1 byte]
		data[4] = comStmtSendLongData

		// Add stmtID [32 bit]
		data[5] = byte(stmt.id)
		data[6] = byte(stmt.id >> 8)
		data[7] = byte(stmt.id >> 16)
		data[8] = byte(stmt.id >> 24)

		// Add paramID [16 bit]
		data[9] = byte(paramID)
		data[10] = byte(paramID >> 8)

		// Send CMD packet
		err = stmt.mc.writePacket(data[:4+pktLen])
		if err == nil {
			argLen -= pktLen - (1 + 4 + 2)
			data = data[pktLen-(1+4+2):]
			continue
		}
		return err

	}

	// Reset Packet Sequence
	stmt.mc.sequence = 0
	return nil
}

// Execute Prepared Statement
// http://dev.mysql.com/doc/internals/en/prepared-statements.html#com-stmt-execute
func (stmt *mysqlStmt) writeExecutePacket(args []driver.Value) error {
	if len(args) != stmt.paramCount {
		return fmt.Errorf(
			"Arguments count mismatch (Got: %d Has: %d",
			len(args),
			stmt.paramCount)
	}

	// Reset packet-sequence
	stmt.mc.sequence = 0

	pktLen := 1 + 4 + 1 + 4 + ((stmt.paramCount + 7) >> 3) + 1 + (stmt.paramCount << 1)
	paramValues := make([][]byte, stmt.paramCount)
	paramTypes := make([]byte, (stmt.paramCount << 1))
	bitMask := uint64(0)
	var i int

	for i = range args {
		// build NULL-bitmap
		if args[i] == nil {
			bitMask += 1 << uint(i)
			paramTypes[i<<1] = fieldTypeNULL
			continue
		}

		// cache types and values
		switch v := args[i].(type) {
		case int64:
			paramTypes[i<<1] = fieldTypeLongLong
			paramValues[i] = uint64ToBytes(uint64(v))
			pktLen += 8
			continue

		case float64:
			paramTypes[i<<1] = fieldTypeDouble
			paramValues[i] = uint64ToBytes(math.Float64bits(v))
			pktLen += 8
			continue

		case bool:
			paramTypes[i<<1] = fieldTypeTiny
			pktLen++
			if v {
				paramValues[i] = []byte{0x01}
			} else {
				paramValues[i] = []byte{0x00}
			}
			continue

		case []byte:
			paramTypes[i<<1] = fieldTypeString
			if len(v) < stmt.mc.maxPacketAllowed-pktLen-(stmt.paramCount-(i+1))*64 {
				paramValues[i] = append(
					lengthEncodedIntegerToBytes(uint64(len(v))),
					v...,
				)
				pktLen += len(paramValues[i])
				continue
			} else {
				err := stmt.writeCommandLongData(i, v)
				if err == nil {
					continue
				}
				return err
			}

		case string:
			paramTypes[i<<1] = fieldTypeString
			if len(v) < stmt.mc.maxPacketAllowed-pktLen-(stmt.paramCount-(i+1))*64 {
				paramValues[i] = append(
					lengthEncodedIntegerToBytes(uint64(len(v))),
					[]byte(v)...,
				)
				pktLen += len(paramValues[i])
				continue
			} else {
				err := stmt.writeCommandLongData(i, []byte(v))
				if err == nil {
					continue
				}
				return err
			}

		case time.Time:
			paramTypes[i<<1] = fieldTypeString
			val := []byte(v.Format(timeFormat))
			paramValues[i] = append(
				lengthEncodedIntegerToBytes(uint64(len(val))),
				val...,
			)
			pktLen += len(paramValues[i])
			continue

		default:
			return fmt.Errorf("Can't convert type: %T", args[i])
		}
	}

	data := make([]byte, pktLen+4)

	// packet header [4 bytes]
	data[0] = byte(pktLen)
	data[1] = byte(pktLen >> 8)
	data[2] = byte(pktLen >> 16)
	data[3] = stmt.mc.sequence

	// command [1 byte]
	data[4] = comStmtExecute

	// statement_id [4 bytes]
	data[5] = byte(stmt.id)
	data[6] = byte(stmt.id >> 8)
	data[7] = byte(stmt.id >> 16)
	data[8] = byte(stmt.id >> 24)

	// flags (0: CURSOR_TYPE_NO_CURSOR) [1 byte]
	//data[9] = 0x00

	// iteration_count (uint32(1)) [4 bytes]
	data[10] = 0x01
	//data[11] = 0x00
	//data[12] = 0x00
	//data[13] = 0x00

	if stmt.paramCount > 0 {
		// NULL-bitmap [(param_count+7)/8 bytes]
		pos := 14 + ((stmt.paramCount + 7) >> 3)
		// Convert bitMask to bytes
		for i = 14; i < pos; i++ {
			data[i] = byte(bitMask >> uint((i-14)<<3))
		}

		// newParameterBoundFlag 1 [1 byte]
		data[pos] = 0x01
		pos++

		// type of parameters [param_count*2 bytes]
		pos += copy(data[pos:], paramTypes)

		// values for the parameters [n bytes]
		for i = range paramValues {
			pos += copy(data[pos:], paramValues[i])
		}
	}

	return stmt.mc.writePacket(data)
}

// http://dev.mysql.com/doc/internals/en/prepared-statements.html#packet-ProtocolBinary::ResultsetRow
func (rc *mysqlRows) readBinaryRow(dest []driver.Value) (err error) {
	data, err := rc.mc.readPacket()
	if err != nil {
		return
	}

	// packet indicator [1 byte]
	if data[0] != iOK {
		// EOF Packet
		if data[0] == iEOF && len(data) == 5 {
			return io.EOF
		} else {
			// Error otherwise
			return rc.mc.handleErrorPacket(data)
		}
	}

	// NULL-bitmap,  [(column-count + 7 + 2) / 8 bytes]
	pos := 1 + (len(dest)+7+2)>>3
	nullBitMap := data[1:pos]

	// values [rest]
	var n int
	var unsigned bool

	for i := range dest {
		// Field is NULL
		// (byte >> bit-pos) % 2 == 1
		if ((nullBitMap[(i+2)>>3] >> uint((i+2)&7)) & 1) == 1 {
			dest[i] = nil
			continue
		}

		unsigned = rc.columns[i].flags&flagUnsigned != 0

		// Convert to byte-coded string
		switch rc.columns[i].fieldType {
		case fieldTypeNULL:
			dest[i] = nil
			continue

		// Numeric Types
		case fieldTypeTiny:
			if unsigned {
				dest[i] = int64(data[pos])
			} else {
				dest[i] = int64(int8(data[pos]))
			}
			pos++
			continue

		case fieldTypeShort, fieldTypeYear:
			if unsigned {
				dest[i] = int64(binary.LittleEndian.Uint16(data[pos : pos+2]))
			} else {
				dest[i] = int64(int16(binary.LittleEndian.Uint16(data[pos : pos+2])))
			}
			pos += 2
			continue

		case fieldTypeInt24, fieldTypeLong:
			if unsigned {
				dest[i] = int64(binary.LittleEndian.Uint32(data[pos : pos+4]))
			} else {
				dest[i] = int64(int32(binary.LittleEndian.Uint32(data[pos : pos+4])))
			}
			pos += 4
			continue

		case fieldTypeLongLong:
			if unsigned {
				val := binary.LittleEndian.Uint64(data[pos : pos+8])
				if val > math.MaxInt64 {
					dest[i] = uint64ToString(val)
				} else {
					dest[i] = int64(val)
				}
			} else {
				dest[i] = int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
			}
			pos += 8
			continue

		case fieldTypeFloat:
			dest[i] = float64(math.Float32frombits(binary.LittleEndian.Uint32(data[pos : pos+4])))
			pos += 4
			continue

		case fieldTypeDouble:
			dest[i] = math.Float64frombits(binary.LittleEndian.Uint64(data[pos : pos+8]))
			pos += 8
			continue

		// Length coded Binary Strings
		case fieldTypeDecimal, fieldTypeNewDecimal, fieldTypeVarChar,
			fieldTypeBit, fieldTypeEnum, fieldTypeSet, fieldTypeTinyBLOB,
			fieldTypeMediumBLOB, fieldTypeLongBLOB, fieldTypeBLOB,
			fieldTypeVarString, fieldTypeString, fieldTypeGeometry:
			var isNull bool
			dest[i], isNull, n, err = readLengthEnodedString(data[pos:])
			pos += n
			if err == nil {
				if !isNull {
					continue
				} else {
					dest[i] = nil
					continue
				}
			}
			return // err

		// Date YYYY-MM-DD
		case fieldTypeDate, fieldTypeNewDate:
			var num uint64
			var isNull bool
			num, isNull, n = readLengthEncodedInteger(data[pos:])

			pos += n

			if num == 0 {
				if isNull {
					dest[i] = nil
					continue
				} else {
					dest[i] = []byte("0000-00-00")
					continue
				}
			} else {
				dest[i] = []byte(fmt.Sprintf("%04d-%02d-%02d",
					binary.LittleEndian.Uint16(data[pos:pos+2]),
					data[pos+2],
					data[pos+3]))
				pos += int(num)
				continue
			}

		// Time [-][H]HH:MM:SS[.fractal]
		case fieldTypeTime:
			var num uint64
			var isNull bool
			num, isNull, n = readLengthEncodedInteger(data[pos:])

			pos += n

			if num == 0 {
				if isNull {
					dest[i] = nil
					continue
				} else {
					dest[i] = []byte("00:00:00")
					continue
				}
			}

			var sign byte
			if data[pos] == 1 {
				sign = byte('-')
			}

			switch num {
			case 8:
				dest[i] = []byte(fmt.Sprintf(
					"%c%02d:%02d:%02d",
					sign,
					uint16(data[pos+1])*24+uint16(data[pos+5]),
					data[pos+6],
					data[pos+7],
				))
				pos += 8
				continue
			case 12:
				dest[i] = []byte(fmt.Sprintf(
					"%c%02d:%02d:%02d.%06d",
					sign,
					uint16(data[pos+1])*24+uint16(data[pos+5]),
					data[pos+6],
					data[pos+7],
					binary.LittleEndian.Uint32(data[pos+8:pos+12]),
				))
				pos += 12
				continue
			default:
				return fmt.Errorf("Invalid TIME-packet length %d", num)
			}

		// Timestamp YYYY-MM-DD HH:MM:SS[.fractal]
		case fieldTypeTimestamp, fieldTypeDateTime:
			var num uint64
			var isNull bool
			num, isNull, n = readLengthEncodedInteger(data[pos:])

			pos += n

			if num == 0 {
				if isNull {
					dest[i] = nil
					continue
				} else {
					dest[i] = []byte("0000-00-00 00:00:00")
					continue
				}
			}

			switch num {
			case 4:
				dest[i] = []byte(fmt.Sprintf(
					"%04d-%02d-%02d 00:00:00",
					binary.LittleEndian.Uint16(data[pos:pos+2]),
					data[pos+2],
					data[pos+3],
				))
				pos += 4
				continue
			case 7:
				dest[i] = []byte(fmt.Sprintf(
					"%04d-%02d-%02d %02d:%02d:%02d",
					binary.LittleEndian.Uint16(data[pos:pos+2]),
					data[pos+2],
					data[pos+3],
					data[pos+4],
					data[pos+5],
					data[pos+6],
				))
				pos += 7
				continue
			case 11:
				dest[i] = []byte(fmt.Sprintf(
					"%04d-%02d-%02d %02d:%02d:%02d.%06d",
					binary.LittleEndian.Uint16(data[pos:pos+2]),
					data[pos+2],
					data[pos+3],
					data[pos+4],
					data[pos+5],
					data[pos+6],
					binary.LittleEndian.Uint32(data[pos+7:pos+11]),
				))
				pos += 11
				continue
			default:
				return fmt.Errorf("Invalid DATETIME-packet length %d", num)
			}

		// Please report if this happens!
		default:
			return fmt.Errorf("Unknown FieldType %d", rc.columns[i].fieldType)
		}
	}

	return
}

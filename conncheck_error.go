// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2026 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import "time"

const idleErrorTimeout = 100 * time.Millisecond
const maxIdleErrorPacketSize = 64 * 1024

// idleError tries to recover an unsolicited ERR packet before discarding an
// idle connection. It never makes a connection reusable, even if the pending
// bytes turn out to be TLS control traffic rather than a MySQL packet.
func (mc *mysqlConn) idleError() error {
	if serverErr := mc.pendingError(); serverErr != nil {
		return serverErr
	}
	return errUnexpectedRead
}

// pendingError reads an ERR packet the server has already sent, bounded by
// idleErrorTimeout and maxIdleErrorPacketSize, and closes the connection. It
// returns nil when no complete ERR packet can be read in time.
func (mc *mysqlConn) pendingError() *MySQLError {
	conn := mc.rawConn
	if conn == nil {
		conn = mc.netConn
	}
	// TLS reads can also write (for example, in response to a KeyUpdate).
	// Close the raw connection at the deadline, independently of ReadTimeout
	// and any deadlines reset by the normal packet reader.
	timer := time.AfterFunc(idleErrorTimeout, func() { conn.Close() })
	defer timer.Stop()
	defer mc.close()
	if err := mc.netConn.SetDeadline(time.Now().Add(idleErrorTimeout)); err != nil {
		return nil
	}

	readNext := mc.readNext
	if mc.compress {
		readNext = func(n int) ([]byte, error) {
			return mc.compIO.readNextLimit(n, maxIdleErrorPacketSize+4)
		}
	}
	// This is diagnostic only: do not enforce sequence numbers, concatenate
	// packets, or apply RejectReadOnly to the original server error.
	header, err := readNext(4)
	if err != nil {
		return nil
	}
	n := getUint24(header[:3])
	if n < 3 || n > maxIdleErrorPacketSize {
		return nil
	}
	data, err := readNext(n)
	if err != nil {
		return nil
	}
	serverErr, err := decodeErrorPacket(data)
	if err != nil {
		return nil
	}
	return serverErr
}

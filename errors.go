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
	"errors"
	"fmt"
	"io"
)

var (
	errMalformPkt  = errors.New("Malformed Packet")
	errPktSync     = errors.New("Commands out of sync. You can't run this command now")
	errPktSyncMul  = errors.New("Commands out of sync. Did you run multiple statements at once?")
	errOldPassword = errors.New("It seems like you are using old_passwords, which is unsupported. See https://github.com/go-sql-driver/mysql/wiki/old_passwords")
	errPktTooLarge = errors.New("Packet for query is too large. You can change this value on the server by adjusting the 'max_allowed_packet' variable.")
)

// error type which represents one or more MySQL warnings
type MySQLWarnings []string

func (mw MySQLWarnings) Error() string {
	var msg string
	for i := range mw {
		if i > 0 {
			msg += "\r\n"
		}
		msg += mw[i]
	}
	return msg
}

func (mc *mysqlConn) getWarnings() (err error) {
	rows, err := mc.Query("SHOW WARNINGS", []driver.Value{})
	if err != nil {
		return
	}

	var warnings = MySQLWarnings{}
	var values = make([]driver.Value, 3)

	for {
		if err = rows.Next(values); err == nil {
			warnings = append(warnings,
				fmt.Sprintf("%s %s: %s", values[0], values[1], values[2]),
			)
		} else if err == io.EOF {
			return warnings
		} else {
			rows.Close()
			return
		}
	}
	return
}

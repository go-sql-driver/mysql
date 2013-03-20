// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2013 Julien Schmidt. All rights reserved.
// http://www.julienschmidt.com
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import "errors"

var (
	errMalformPkt  = errors.New("Malformed Packet")
	errPktSync     = errors.New("Commands out of sync. You can't run this command now")
	errPktSyncMul  = errors.New("Commands out of sync. Did you run multiple statements at once?")
	errOldPassword = errors.New("It seems like you are using old_passwords, which is unsupported. See https://github.com/go-sql-driver/mysql/wiki/old_passwords")
	errPktTooLarge = errors.New("Packet for query is too large. You can change this value on the server by adjusting the 'max_allowed_packet' variable.")
)

// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2012 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import "database/sql/driver"

type mysqlTx struct {
	mc *mysqlConn
}

func (tx *mysqlTx) Commit() (err error) {
	if mc := tx.mc; mc == nil || mc.netConn == nil {
		errLog.Print(errInvalidConn)
		tx.mc = nil
		return driver.ErrBadConn
	}

	err = tx.mc.exec("COMMIT")
	tx.mc = nil
	return
}

func (tx *mysqlTx) Rollback() (err error) {
	if mc := tx.mc; mc == nil || mc.netConn == nil {
		errLog.Print(errInvalidConn)
		tx.mc = nil
		return driver.ErrBadConn
	}

	err = tx.mc.exec("ROLLBACK")
	tx.mc = nil
	return
}

// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2012 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

type mysqlTx struct {
	mc *mysqlConn
}

// Commit implements driver.Tx interface
func (tx *mysqlTx) Commit() (err error) {
	if tx.mc == nil || tx.mc.netConn == nil {
		return ErrInvalidConn
	}
	err = tx.mc.exec(backgroundCtx(), "COMMIT")
	tx.mc = nil
	return
}

// Rollback implements driver.Tx interface
func (tx *mysqlTx) Rollback() (err error) {
	if tx.mc == nil || tx.mc.netConn == nil {
		return ErrInvalidConn
	}
	err = tx.mc.exec(backgroundCtx(), "ROLLBACK")
	tx.mc = nil
	return
}

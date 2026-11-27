// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2012 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import "slices"

import "database/sql/driver"

// Result exposes data not available through *connection.Result.
//
// This is accessible by executing statements using sql.Conn.Raw() and
// downcasting the returned result:
//
//	res, err := rawConn.Exec(...)
//	res.(mysql.Result).AllRowsAffected()
//
// When the server returns an error (*MySQLError), Exec on the driver
// connection also returns the Result read so far. When a statement fails,
// its entry is last. Each statement that returns one result (INSERT,
// UPDATE, DELETE, a SELECT) has one entry, so with only such statements
// len(res.AllRowsAffected()) == n means statement n failed. A statement
// that returns several results, such as a CALL, has several entries, and
// the count then does not identify the failing statement. Neither does an
// error the server returns before running any statement, such as 1153 (the
// query exceeds max_allowed_packet): the Result then has one entry although
// nothing ran. database/sql discards this Result; use
// [database/sql.Conn.Raw] or the driver connection directly.
type Result interface {
	driver.Result
	// AllRowsAffected returns a slice containing the affected rows for each
	// result read.
	AllRowsAffected() []int64
	// AllLastInsertIds returns a slice containing the last inserted ID for each
	// result read.
	AllLastInsertIds() []int64
}

type mysqlResult struct {
	// One entry in both slices is created for every executed statement result.
	affectedRows []int64
	insertIds    []int64
}

func (res *mysqlResult) LastInsertId() (int64, error) {
	return res.insertIds[len(res.insertIds)-1], nil
}

func (res *mysqlResult) RowsAffected() (int64, error) {
	return res.affectedRows[len(res.affectedRows)-1], nil
}

func (res *mysqlResult) AllLastInsertIds() []int64 {
	return slices.Clone(res.insertIds) // defensive copy
}

func (res *mysqlResult) AllRowsAffected() []int64 {
	return slices.Clone(res.affectedRows) // defensive copy
}

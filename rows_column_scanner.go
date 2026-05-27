// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2025 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

//go:build go1.27

package mysql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
)

// NextRow implements driver.RowsColumnScanner.
func (rows *binaryRows) NextRow() error {
	if mc := rows.mc; mc != nil {
		if err := mc.error(); err != nil {
			return err
		}
		ncols := len(rows.rs.columns)
		if len(rows.currentRow) != ncols {
			rows.currentRow = make([]driver.Value, ncols)
		}
		return rows.readRow(rows.currentRow)
	}
	return io.EOF
}

// NextRow implements driver.RowsColumnScanner.
func (rows *textRows) NextRow() error {
	if mc := rows.mc; mc != nil {
		if err := mc.error(); err != nil {
			return err
		}
		ncols := len(rows.rs.columns)
		if len(rows.currentRow) != ncols {
			rows.currentRow = make([]driver.Value, ncols)
		}
		return rows.readRow(rows.currentRow)
	}
	return io.EOF
}

// ScanColumn implements driver.RowsColumnScanner.
func (rows *mysqlRows) ScanColumn(scanCtx driver.ScanContext, i int, dest any) error {
	if i < 0 || i >= len(rows.currentRow) {
		return fmt.Errorf("mysql: column index %d out of range [0, %d)", i, len(rows.currentRow))
	}
	return sql.ConvertAssign(scanCtx, dest, rows.currentRow[i])
}

// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2026 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import "testing"

// Regression for #1733: indexing into an empty result slice used to panic.
func TestEmptyResult(t *testing.T) {
	r := &mysqlResult{}
	if id, err := r.LastInsertId(); err != nil || id != 0 {
		t.Errorf("LastInsertId() = (%d, %v), want (0, nil)", id, err)
	}
	if rows, err := r.RowsAffected(); err != nil || rows != 0 {
		t.Errorf("RowsAffected() = (%d, %v), want (0, nil)", rows, err)
	}
}

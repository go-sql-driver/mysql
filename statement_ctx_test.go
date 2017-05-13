// +build go1.8

// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2012 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.package mysql

package mysql

import (
	"database/sql/driver"
)

var (
	_ driver.StmtExecContext  = &mysqlStmt{}
	_ driver.StmtQueryContext = &mysqlStmt{}
)

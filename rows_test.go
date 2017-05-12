// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2017 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"database/sql/driver"
)

// Ensure that all the driver interfaces are implemented
var (
	_ driver.Rows                           = &binaryRows{}
	_ driver.Rows                           = &textRows{}
	_ driver.RowsColumnTypeDatabaseTypeName = &binaryRows{}
	_ driver.RowsColumnTypeDatabaseTypeName = &textRows{}
	_ driver.RowsColumnTypeNullable         = &binaryRows{}
	_ driver.RowsColumnTypeNullable         = &textRows{}
	_ driver.RowsColumnTypeScanType         = &binaryRows{}
	_ driver.RowsColumnTypeScanType         = &textRows{}
	_ driver.RowsNextResultSet              = &binaryRows{}
	_ driver.RowsNextResultSet              = &textRows{}
)

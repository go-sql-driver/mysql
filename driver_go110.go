// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2012 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

// +build go1.10

package mysql

import (
	"database/sql/driver"
)

// OpenConnector implements driver.DriverContext.
func (d MySQLDriver) OpenConnector(dsn string) (driver.Connector, error) {
	c, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}

	return Connector{
		Config: c,
	}, nil
}

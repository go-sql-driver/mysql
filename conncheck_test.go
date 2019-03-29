// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2013 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

// +build go1.10,!windows

package mysql

import (
	"testing"
	"time"
)

func TestStaleConnectionChecks(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("SET @@SESSION.wait_timeout = 2")

		if err := dbt.db.Ping(); err != nil {
			dbt.Fatal(err)
		}

		// wait for MySQL to close our connection
		time.Sleep(3 * time.Second)

		tx, err := dbt.db.Begin()
		if err != nil {
			dbt.Fatal(err)
		}

		if err := tx.Rollback(); err != nil {
			dbt.Fatal(err)
		}
	})
}

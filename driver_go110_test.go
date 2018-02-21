// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2018 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

// +build go1.10

package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"sync"
	"testing"
)

type Connector struct {
	m     sync.Mutex
	mysql *MySQLConnector
}

func (c *Connector) Connect(cxt context.Context) (driver.Conn, error) {
	var err error

	if c.mysql == nil {
		c.mysql = c.init()
	}

	//Just use the global DSN because we just want to test the connector
	//interface and we do not care about any custom functionality in the Connector
	c.m.Lock()
	c.mysql.Cfg, err = ParseDSN(dsn)
	c.m.Unlock()
	if err != nil {
		println(err)
		return nil, err
	}

	return c.mysql.Connect(cxt)
}

func (c *Connector) Driver() driver.Driver {
	return c.mysql.Driver()
}

func (c *Connector) init() *MySQLConnector {
	return &MySQLConnector{}
}

func runtestsWithConnector(t *testing.T, tests ...func(dbt *DBTest)) {
	if !available {
		t.Skipf("MySQL server not running on %s", netAddr)
	}

	connector := &Connector{}

	db := sql.OpenDB(connector)
	if err := db.Ping(); err != nil {
		db.Close()
		t.Fatalf("error connecting: %s", err.Error())
	}
	defer db.Close()

	dbt := &DBTest{t, db}
	for _, test := range tests {
		test(dbt)
		dbt.db.Exec("DROP TABLE IF EXISTS test")
	}

}

func TestPingWithConnector(t *testing.T) {
	runtestsWithConnector(t, func(dbt *DBTest) {
		if err := dbt.db.Ping(); err != nil {
			dbt.fail("Ping With Connector", "Ping With Connector", err)
		}
	})
}

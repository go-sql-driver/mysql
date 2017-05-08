// +build go1.8

// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2012 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"context"
	"database/sql/driver"
)

// The definitions below are for compatibility with older Go versions.
// See ctx_backport.go for the definitions used in older Go versions.

type txOptions driver.TxOptions

type mysqlContext context.Context

func backgroundCtx() mysqlContext {
	return context.Background()
}

var deadlineExceeded = context.DeadlineExceeded

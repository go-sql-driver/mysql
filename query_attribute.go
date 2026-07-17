// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2026 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import "fmt"

// QueryAttribute is a per-query key-value pair sent using MySQL query
// attributes. Query attributes are not SQL bind parameters and apply only to
// the Exec or Query call in which they are supplied.
//
// Currently, Value must be a string.
type QueryAttribute struct {
	Name  string
	Value any
}

func (attr *QueryAttribute) validate() error {
	if attr.Name == "" {
		return fmt.Errorf("mysql: query attribute name must not be empty")
	}
	if _, ok := attr.Value.(string); !ok {
		return fmt.Errorf("mysql: unsupported query attribute value type %T", attr.Value)
	}
	return nil
}

// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2017 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import "testing"

type customString string

func TestConvertValueCustomTypes(t *testing.T) {
	var cstr customString = "string"
	c := converter{}
	if _, err := c.ConvertValue(cstr); err != nil {
		t.Errorf("custom string type should be valid")
	}
}

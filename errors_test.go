// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2013 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"bytes"
	"log"
	"testing"
)

func TestSetLogger(t *testing.T) {
	previous := errLog
	defer func() {
		errLog = previous
	}()
	const expected = "prefix: test\n"
	buffer := bytes.NewBuffer(make([]byte, 0, 64))
	logger := log.New(buffer, "prefix: ", 0)
	SetLogger(logger)
	errLog.Print("test")
	if actual := buffer.String(); actual != expected {
		t.Errorf("expected %q, got %q", expected, actual)
	}
}

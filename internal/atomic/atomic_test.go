// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2017 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package atomic

import (
	"errors"
	"testing"
)

var (
	errOne = errors.New("one")
	errTwo = errors.New("two")
)

func TestAtomicBool(t *testing.T) {
	var b Bool
	if b.IsSet() {
		t.Fatal("Expected value to be false")
	}

	b.Set(true)
	if b.value != 1 {
		t.Fatal("Set(true) did not set value to 1")
	}
	if !b.IsSet() {
		t.Fatal("Expected value to be true")
	}

	b.Set(true)
	if !b.IsSet() {
		t.Fatal("Expected value to be true")
	}

	b.Set(false)
	if b.value != 0 {
		t.Fatal("Set(false) did not set value to 0")
	}
	if b.IsSet() {
		t.Fatal("Expected value to be false")
	}

	b.Set(false)
	if b.IsSet() {
		t.Fatal("Expected value to be false")
	}
	if b.TrySet(false) {
		t.Fatal("Expected TrySet(false) to fail")
	}
	if !b.TrySet(true) {
		t.Fatal("Expected TrySet(true) to succeed")
	}
	if !b.IsSet() {
		t.Fatal("Expected value to be true")
	}

	b.Set(true)
	if !b.IsSet() {
		t.Fatal("Expected value to be true")
	}
	if b.TrySet(true) {
		t.Fatal("Expected TrySet(true) to fail")
	}
	if !b.TrySet(false) {
		t.Fatal("Expected TrySet(false) to succeed")
	}
	if b.IsSet() {
		t.Fatal("Expected value to be false")
	}

	b._noCopy.Lock() // we've "tested" it ¯\_(ツ)_/¯
}

func TestAtomicError(t *testing.T) {
	var e Error
	if e.Value() != nil {
		t.Fatal("Expected value to be nil")
	}

	e.Set(errOne)
	if v := e.Value(); v != errOne {
		if v == nil {
			t.Fatal("Value is still nil")
		}
		t.Fatal("Error did not match")
	}
	e.Set(errTwo)
	if e.Value() == errOne {
		t.Fatal("Error still matches old error")
	}
	if v := e.Value(); v != errTwo {
		t.Fatal("Error did not match")
	}
}

// Go MySQL Driver - A MySQL-Driver for Go's database/sql package.
//
// Copyright 2022 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.
//go:build !go1.19
// +build !go1.19

package mysql

import (
	"testing"
)

func TestAtomicBool(t *testing.T) {
	var ab atomicBool
	if ab.Load() {
		t.Fatal("Expected value to be false")
	}

	ab.Store(true)
	if ab.value != 1 {
		t.Fatal("Set(true) did not set value to 1")
	}
	if !ab.Load() {
		t.Fatal("Expected value to be true")
	}

	ab.Store(true)
	if !ab.Load() {
		t.Fatal("Expected value to be true")
	}

	ab.Store(false)
	if ab.value != 0 {
		t.Fatal("Set(false) did not set value to 0")
	}
	if ab.Load() {
		t.Fatal("Expected value to be false")
	}

	ab.Store(false)
	if ab.Load() {
		t.Fatal("Expected value to be false")
	}
	if ab.Swap(false) {
		t.Fatal("Expected the old value to be false")
	}
	if ab.Swap(true) {
		t.Fatal("Expected the old value to be false")
	}
	if !ab.Load() {
		t.Fatal("Expected value to be true")
	}

	ab.Store(true)
	if !ab.Load() {
		t.Fatal("Expected value to be true")
	}
	if !ab.Swap(true) {
		t.Fatal("Expected the old value to be true")
	}
	if !ab.Swap(false) {
		t.Fatal("Expected the old value to be true")
	}
	if ab.Load() {
		t.Fatal("Expected value to be false")
	}
}

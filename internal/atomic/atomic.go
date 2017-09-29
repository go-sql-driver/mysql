// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2017 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package atomic

import (
	"sync/atomic"
)

// noCopy may be embedded into structs which must not be copied
// after the first use.
//
// See https://github.com/golang/go/issues/8005#issuecomment-190753527
// for details.
type noCopy struct{}

// Lock is a no-op used by -copylocks checker from `go vet`.
func (*noCopy) Lock() {}

// Bool is a wrapper around uint32 for usage as a boolean value with
// atomic access.
type Bool struct {
	_noCopy noCopy
	value   uint32
}

// IsSet returns wether the current boolean value is true
func (b *Bool) IsSet() bool {
	return atomic.LoadUint32(&b.value) > 0
}

// Set sets the value of the bool regardless of the previous value
func (b *Bool) Set(value bool) {
	if value {
		atomic.StoreUint32(&b.value, 1)
	} else {
		atomic.StoreUint32(&b.value, 0)
	}
}

// TrySet sets the value of the bool and returns wether the value changed
func (b *Bool) TrySet(value bool) bool {
	if value {
		return atomic.SwapUint32(&b.value, 1) == 0
	}
	return atomic.SwapUint32(&b.value, 0) > 0
}

// Error is a wrapper for atomically accessed error values
type Error struct {
	_noCopy noCopy
	value   atomic.Value
}

// Set sets the error value regardless of the previous value.
// The value must not be nil
func (e *Error) Set(value error) {
	e.value.Store(value)
}

// Value returns the current error value
func (e *Error) Value() error {
	if v := e.value.Load(); v != nil {
		// this will panic if the value doesn't implement the error interface
		return v.(error)
	}
	return nil
}

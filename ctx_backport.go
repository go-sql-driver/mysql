// +build !go1.8

// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2012 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"time"
)

// txOptions is defined for compatibility with Go 1.8's driver.TxOptions struct.
type txOptions struct {
	Isolation int
	ReadOnly  bool
}

// mysqlContext is a copy of context.Context from Go 1.7 and later.
type mysqlContext interface {
	Deadline() (deadline time.Time, ok bool)

	Done() <-chan struct{}

	Err() error

	Value(key interface{}) interface{}
}

// emptyCtx is copied from Go 1.7's context package.
type emptyCtx int

func (*emptyCtx) Deadline() (deadline time.Time, ok bool) {
	return
}

func (*emptyCtx) Done() <-chan struct{} {
	return nil
}

func (*emptyCtx) Err() error {
	return nil
}

func (*emptyCtx) Value(key interface{}) interface{} {
	return nil
}

func (e *emptyCtx) String() string {
	return "context.Background"
}

var background = new(emptyCtx)

func backgroundCtx() mysqlContext {
	return background
}

var deadlineExceeded = deadlineExceededError{}

// deadlineExceededError is copied from Go 1.7's context package.
type deadlineExceededError struct{}

func (deadlineExceededError) Error() string   { return "context deadline exceeded" }
func (deadlineExceededError) Timeout() bool   { return true }
func (deadlineExceededError) Temporary() bool { return true }

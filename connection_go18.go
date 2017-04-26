// +build go1.8

// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2017 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"context"
	"database/sql/driver"
	"net"
	"time"
)

func (mc *mysqlConn) Ping(ctx context.Context) error {
	var err error

	if ctx == nil {
		return mc.pingImpl()
	}
	mc.ctx = ctx
	defer func() {
		mc.ctx = nil
	}()

	if deadline, ok := ctx.Deadline(); ok {
		if err = mc.netConn.SetDeadline(deadline); err != nil {
			return err
		}
	}

	if ctx.Done() == nil {
		return mc.pingImpl()
	}

	result := make(chan error)
	go func() {
		result <- mc.pingImpl()
	}()

	select {
	case <-ctx.Done():
		// Because buffer.fill() and mysqlConn.writePacket() may overwrite the
		// deadline of read/write again and again in the above goroutine,
		// it has to use a loop to make sure SetDeadline() works.
	UpdateDeadlineLoop:
		for {
			// Copy it as a local variable because mc.netConn may be closed and
			// assigned nil in the above goroutine.
			netConn := mc.netConn
			if netConn != nil {
				errDeadline := netConn.SetDeadline(time.Now())
				errLog.Print(errDeadline)
			}
			select {
			case <-time.After(200 * time.Millisecond):
				// Prevent from leaking the above goroutine.
			case err = <-result:
				break UpdateDeadlineLoop
			}
		}
	case err = <-result:
	}

	if netErr, ok := err.(net.Error); ok {
		// We don't know where it timed out and it may leave some redundant data
		// in the connection so make it to be closed by DB.puConn() of the
		// caller.
		if netErr.Timeout() {
			return driver.ErrBadConn
		}
	}
	return err
}

func (mc *mysqlConn) pingImpl() error {
	if err := mc.writeCommandPacket(comPing); err != nil {
		return err
	}

	_, err := mc.readResultOK()
	return err
}

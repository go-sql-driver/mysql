// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2013 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"errors"
	"fmt"
	"log"
	"os"
)

// Various errors the driver might return. Can change between driver versions.
var (
	ErrInvalidConn       = errors.New("invalid connection")
	ErrMalformPkt        = errors.New("malformed packet")
	ErrNoTLS             = errors.New("TLS requested but server does not support TLS")
	ErrCleartextPassword = errors.New("this user requires clear text authentication. If you still want to use it, please add 'allowCleartextPasswords=1' to your DSN")
	ErrNativePassword    = errors.New("this user requires mysql native password authentication")
	ErrOldPassword       = errors.New("this user requires old password authentication. If you still want to use it, please add 'allowOldPasswords=1' to your DSN. See also https://github.com/go-sql-driver/mysql/wiki/old_passwords")
	ErrUnknownPlugin     = errors.New("this authentication plugin is not supported")
	ErrOldProtocol       = errors.New("MySQL server does not support required protocol 41+")
	ErrPktSync           = errors.New("commands out of sync. You can't run this command now")
	ErrPktSyncMul        = errors.New("commands out of sync. Did you run multiple statements at once?")
	ErrPktTooLarge       = errors.New("packet for query is too large. Try adjusting the `Config.MaxAllowedPacket`")
	ErrBusyBuffer        = errors.New("busy buffer")

	// errBadConnNoWrite is used for connection errors where nothing was sent to the database yet.
	// If this happens first in a function starting a database interaction, it should be replaced by driver.ErrBadConn
	// to trigger a resend.
	// See https://github.com/go-sql-driver/mysql/pull/302
	errBadConnNoWrite = errors.New("bad connection")
)

var defaultLogger = Logger(log.New(os.Stderr, "[mysql] ", log.Ldate|log.Ltime|log.Lshortfile))

// traceLogger is used for debug trace log.
var traceLogger *log.Logger

func init() {
	if debugTrace {
		traceLogger = log.New(os.Stderr, "[mysql.trace]", log.Lmicroseconds|log.Lshortfile)
	}
}

func trace(format string, v ...any) {
	if debugTrace {
		traceLogger.Printf(format, v...)
	}
}

// Logger is used to log critical error messages.
type Logger interface {
	Print(v ...any)
}

func (mc *mysqlConn) logf(format string, v ...any) {
	mc.cfg.Logger.Print(fmt.Sprintf(format, v...))
}

// NopLogger is a nop implementation of the Logger interface.
type NopLogger struct{}

// Print implements Logger interface.
func (nl *NopLogger) Print(_ ...any) {}

// SetLogger is used to set the default logger for critical errors.
// The initial logger is os.Stderr.
func SetLogger(logger Logger) error {
	if logger == nil {
		return errors.New("logger is nil")
	}
	defaultLogger = logger
	return nil
}

// MySQLError is an error type which represents a single MySQL error
type MySQLError struct {
	Number   uint16
	SQLState [5]byte
	Message  string
}

func (me *MySQLError) Error() string {
	if me.SQLState != [5]byte{} {
		return fmt.Sprintf("Error %d (%s): %s", me.Number, me.SQLState, me.Message)
	}

	return fmt.Sprintf("Error %d: %s", me.Number, me.Message)
}

func (me *MySQLError) Is(err error) bool {
	if merr, ok := err.(*MySQLError); ok {
		return merr.Number == me.Number
	}
	return false
}

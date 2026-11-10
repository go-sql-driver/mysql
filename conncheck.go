// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2019 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || solaris || illumos

package mysql

import (
	"io"
	"net"
	"syscall"
)

func connCheck(conn net.Conn) error {
	var sysErr error

	sysConn, ok := conn.(syscall.Conn)
	if !ok {
		return nil
	}
	rawConn, err := sysConn.SyscallConn()
	if err != nil {
		return err
	}

	err = rawConn.Read(func(fd uintptr) bool {
		var buf [1]byte
		// Leave the bytes for the TLS/compression layers to decode.
		n, _, err := syscall.Recvfrom(int(fd), buf[:], syscall.MSG_PEEK)
		if err == syscall.ENOTSOCK {
			// Preserve the original liveness check for pipe-backed dialers.
			// If Read consumes data, reject the connection without attempting
			// the diagnostic, which requires an intact (peeked) packet.
			n, err = syscall.Read(int(fd), buf[:])
			if n > 0 {
				sysErr = errUnexpectedReadNonSocket
				return true
			}
		}
		switch {
		case n == 0 && err == nil:
			sysErr = io.EOF
		case n > 0:
			sysErr = errUnexpectedRead
		case err == syscall.EAGAIN || err == syscall.EWOULDBLOCK:
			sysErr = nil
		default:
			sysErr = err
		}
		return true
	})
	if err != nil {
		return err
	}

	return sysErr
}

// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2019 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || solaris || illumos
// +build linux darwin dragonfly freebsd netbsd openbsd solaris illumos

package mysql

import (
	"errors"
	"fmt"
	"net"
	"syscall"

	"golang.org/x/sys/unix"
)

var errUnexpectedEvent = errors.New("recieved unexpected event")

func connCheck(conn net.Conn) error {
	sysConn, ok := conn.(syscall.Conn)
	if !ok {
		return nil
	}
	rawConn, err := sysConn.SyscallConn()
	if err != nil {
		return err
	}

	var pollErr error
	err = rawConn.Control(func(fd uintptr) {
		fds := []unix.PollFd{
			{Fd: int32(fd), Events: unix.POLLIN | unix.POLLERR},
		}
		n, err := unix.Poll(fds, 0)
		if err != nil {
			pollErr = fmt.Errorf("poll: %w", err)
		}
		if n > 0 {
			// fmt.Errorf("poll: %v", fds[0].Revents)
			pollErr = errUnexpectedEvent
		}
	})
	if err != nil {
		return err
	}
	return pollErr
}

// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2026 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"io"
	"net"
	"syscall"
	"unsafe"
)

var procWSAPoll = syscall.NewLazyDLL("ws2_32.dll").NewProc("WSAPoll")

// WSAPOLLFD uses a pointer-sized SOCKET followed by two SHORT fields.
type wsaPollFD struct {
	fd      syscall.Handle
	events  int16
	revents int16
}

func connCheck(conn net.Conn) error {
	sysConn, ok := conn.(syscall.Conn)
	if !ok {
		return nil
	}
	rawConn, err := sysConn.SyscallConn()
	if err != nil {
		return err
	}

	var sysErr error
	err = rawConn.Read(func(fd uintptr) bool {
		const (
			pollErr        = 0x0001
			pollHup        = 0x0002
			pollNval       = 0x0004
			pollRdNorm     = 0x0100
			msgPeek        = 0x0002
			wsaENotSock    = syscall.Errno(10038)
			wsaEWouldBlock = syscall.Errno(10035)
		)
		// Go's sockets use overlapped I/O, so a synchronous WSARecv can
		// block. Poll first with a zero timeout. RawConn.Read holds the
		// read lock, preventing another Go reader from consuming the data.
		pfd := wsaPollFD{fd: syscall.Handle(fd), events: pollRdNorm}
		n, _, pollErrno := procWSAPoll.Call(uintptr(unsafe.Pointer(&pfd)), 1, 0)
		switch {
		case int32(n) == -1:
			sysErr = pollErrno
			return true
		case n == 0:
			return true
		case pfd.revents&pollNval != 0:
			sysErr = wsaENotSock
			return true
		case pfd.revents&(pollRdNorm|pollErr|pollHup) == 0:
			return true
		}

		// Peek even when POLLHUP is set: data can precede a peer's FIN.
		var buf [1]byte
		wsaBuf := syscall.WSABuf{Len: uint32(len(buf)), Buf: &buf[0]}
		var received uint32
		flags := uint32(msgPeek)
		err := syscall.WSARecv(syscall.Handle(fd), &wsaBuf, 1, &received, &flags, nil, nil)
		switch {
		case received > 0:
			sysErr = errUnexpectedRead
		case err == nil:
			sysErr = io.EOF
		case err == wsaEWouldBlock:
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

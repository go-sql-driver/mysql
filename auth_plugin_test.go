// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2023 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import "testing"

func TestSimpleAuthRejectsContinuation(t *testing.T) {
	nextPacket, err := (SimpleAuth{}).ContinuationAuth(nil, nil, NewConfig())
	if err != ErrMalformPkt {
		t.Fatalf("expected ErrMalformPkt, got %v", err)
	}
	if nextPacket != nil {
		t.Errorf("expected no response packet, got %v", nextPacket)
	}
}

// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2013 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"database/sql/driver"
	"reflect"
	"testing"
	"time"
)

func Test_mysqlConn_appendEncode(t *testing.T) {
	type args struct {
		buf []byte
		x   driver.Value
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			name: "test String",
			args: args{[]byte{}, "test"},
			want: []byte("test"),
		},
		{
			name: "test Int64",
			args: args{[]byte{}, driver.Value(int64(42))},
			want: []byte("42"),
		},
		{
			name: "test Uint64",
			args: args{[]byte{}, driver.Value(uint64(42))},
			want: []byte("42"),
		},
		{
			name: "test Fload64",
			args: args{[]byte{}, driver.Value(float64(42.23))},
			want: []byte("42.23"),
		},
		{
			name: "test Bool",
			args: args{[]byte{}, driver.Value(bool(true))},
			want: []byte("1"),
		},
		{
			name: "test BoolFalse",
			args: args{[]byte{}, driver.Value(bool(false))},
			want: []byte("0"),
		},
		{
			name: "test nil",
			args: args{[]byte{}, driver.Value(nil)},
			want: []byte("\\N"),
		},
		{
			name: "test TimeNULL",
			args: args{[]byte{}, driver.Value(time.Time{})},
			want: []byte("0000-00-00"),
		},
		{
			name: "test Time",
			args: args{[]byte{}, driver.Value(time.Date(2014, time.December, 31, 12, 13, 24, 0, time.UTC))},
			want: []byte("2014-12-31 12:13:24"),
		},
		{
			name: "test byteNil",
			args: args{[]byte{}, driver.Value([]byte(nil))},
			want: []byte("\\N"),
		},
		{
			name: "test byte",
			args: args{[]byte{}, driver.Value([]byte("test"))},
			want: []byte("test"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := &mysqlConn{
				cfg:              NewConfig(),
				maxAllowedPacket: defaultMaxAllowedPacket,
			}
			if got := mc.appendEncode(tt.args.buf, tt.args.x); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("mysqlConn.appendEncode() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_appendEscaped(t *testing.T) {
	type args struct {
		buf []byte
		v   string
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			name: "test1",
			args: args{[]byte{}, "test"},
			want: []byte("test"),
		},
		{
			name: "test TAB",
			args: args{[]byte{}, "t\test"},
			want: []byte("t\\test"),
		},
		{
			name: "test LF",
			args: args{[]byte{}, "t\nest"},
			want: []byte("t\\nest"),
		},
		{
			name: "test CR",
			args: args{[]byte{}, "t\rest"},
			want: []byte("t\\rest"),
		},
		{
			name: "test BackSlash",
			args: args{[]byte{}, "t\\est"},
			want: []byte("t\\\\est"),
		},
		{
			name: "test All",
			args: args{[]byte{}, "t\t\n\r\\est"},
			want: []byte("t\\t\\n\\r\\\\est"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := appendEscaped(tt.args.buf, tt.args.v); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("appendEscaped() = %v, want %v", got, tt.want)
			}
		})
	}
}

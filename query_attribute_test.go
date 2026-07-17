// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2026 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"slices"
	"testing"
)

func TestNamedValueToValueQueryAttributes(t *testing.T) {
	args, attrs, err := namedValueToValue([]driver.NamedValue{
		{Ordinal: 1, Value: int64(42)},
		{Ordinal: 2, Value: QueryAttribute{Name: "trace", Value: "abc"}},
		{Ordinal: 3, Value: "bound"},
		{Ordinal: 4, Value: QueryAttribute{Name: "region", Value: "west"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	wantArgs := []driver.Value{int64(42), "bound"}
	if !slices.Equal(args, wantArgs) {
		t.Fatalf("args = %#v; want %#v", args, wantArgs)
	}
	wantAttrs := []QueryAttribute{
		{Name: "trace", Value: "abc"},
		{Name: "region", Value: "west"},
	}
	if !slices.Equal(attrs, wantAttrs) {
		t.Fatalf("attrs = %#v; want %#v", attrs, wantAttrs)
	}
}

func TestQueryAttributeValidation(t *testing.T) {
	tests := []struct {
		name string
		attr QueryAttribute
		err  string
	}{
		{name: "empty name", attr: QueryAttribute{Value: "value"}, err: "mysql: query attribute name must not be empty"},
		{name: "unsupported value", attr: QueryAttribute{Name: "name", Value: 42}, err: "mysql: unsupported query attribute value type int"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := namedValueToValue([]driver.NamedValue{{Ordinal: 1, Value: test.attr}})
			if err == nil || err.Error() != test.err {
				t.Fatalf("want error %q, got %v", test.err, err)
			}
		})
	}
}

func TestWriteQueryPacketWithAttributes(t *testing.T) {
	conn, mc := newRWMockConn(0)
	mc.capabilities = clientQueryAttributes

	err := mc.writeQueryPacket("SELECT 1", []QueryAttribute{{Name: "trace", Value: "abc"}})
	if err != nil {
		t.Fatal(err)
	}

	if len(conn.written) != int(conn.written[0])+4 {
		t.Fatalf("packet length header is %d, packet length is %d", conn.written[0], len(conn.written)-4)
	}
	want := []byte{
		comQuery,
		1, 1, // parameter count and parameter set count
		0, // null bitmap
		1, // new parameters bound
		byte(fieldTypeString), 0, 5, 't', 'r', 'a', 'c', 'e',
		3, 'a', 'b', 'c',
		'S', 'E', 'L', 'E', 'C', 'T', ' ', '1',
	}
	if got := conn.written[4:]; !slices.Equal(got, want) {
		t.Fatalf("packet = %#v; want %#v", got, want)
	}
}

func TestWriteQueryPacketWithoutAttributes(t *testing.T) {
	conn, mc := newRWMockConn(0)
	mc.capabilities = clientQueryAttributes

	err := mc.writeQueryPacket("DO 1", nil)
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{comQuery, 0, 1, 'D', 'O', ' ', '1'}
	if got := conn.written[4:]; !slices.Equal(got, want) {
		t.Fatalf("packet = %#v; want %#v", got, want)
	}
}

func TestWriteQueryPacketAttributesDoNotPersist(t *testing.T) {
	conn, mc := newRWMockConn(0)
	mc.capabilities = clientQueryAttributes

	if err := mc.writeQueryPacket("DO 1", []QueryAttribute{{Name: "trace", Value: "abc"}}); err != nil {
		t.Fatal(err)
	}
	conn.written = nil
	if err := mc.writeQueryPacket("DO 2", nil); err != nil {
		t.Fatal(err)
	}

	want := []byte{comQuery, 0, 1, 'D', 'O', ' ', '2'}
	if got := conn.written[4:]; !slices.Equal(got, want) {
		t.Fatalf("packet = %#v; want %#v", got, want)
	}
}

func TestWriteQueryPacketIgnoresAttributesWithoutCapability(t *testing.T) {
	conn, mc := newRWMockConn(0)

	err := mc.writeQueryPacket("DO 1", []QueryAttribute{{Name: "trace", Value: "abc"}})
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{comQuery, 'D', 'O', ' ', '1'}
	if got := conn.written[4:]; !slices.Equal(got, want) {
		t.Fatalf("packet = %#v; want %#v", got, want)
	}
}

func TestWriteExecutePacketWithQueryAttribute(t *testing.T) {
	conn, mc := newRWMockConn(0)
	mc.capabilities = clientQueryAttributes
	mc.serverVersion = [3]int{8, 0, 26}
	stmt := mysqlStmt{mc: mc, id: 7, paramCount: 1}

	err := stmt.writeExecutePacket(
		[]driver.Value{"bound"},
		[]QueryAttribute{{Name: "trace", Value: "abc"}},
	)
	if err != nil {
		t.Fatal(err)
	}

	want := []byte{
		comStmtExecute,
		7, 0, 0, 0,
		parameterCountAvailable,
		1, 0, 0, 0,
		2,                           // parameter count
		0,                           // null bitmap
		1,                           // new parameters bound
		byte(fieldTypeString), 0, 0, // ordinary bind has an empty name
		byte(fieldTypeString), 0, 5, 't', 'r', 'a', 'c', 'e',
		5, 'b', 'o', 'u', 'n', 'd',
		3, 'a', 'b', 'c',
	}
	if got := conn.written[4:]; !slices.Equal(got, want) {
		t.Fatalf("packet = %#v; want %#v", got, want)
	}
}

func TestWriteExecutePacketIgnoresUnsafeQueryAttribute(t *testing.T) {
	conn, mc := newRWMockConn(0)
	mc.capabilities = clientQueryAttributes
	mc.serverVersion = [3]int{8, 0, 25}
	stmt := mysqlStmt{mc: mc, id: 7}

	err := stmt.writeExecutePacket(nil, []QueryAttribute{{Name: "trace", Value: "abc"}})
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{
		comStmtExecute,
		7, 0, 0, 0,
		0,
		1, 0, 0, 0,
	}
	if got := conn.written[4:]; !slices.Equal(got, want) {
		t.Fatalf("packet = %#v; want %#v", got, want)
	}
}

func TestParseServerVersion(t *testing.T) {
	tests := []struct {
		version string
		want    [3]int
	}{
		{version: "8.0.26-commercial", want: [3]int{8, 0, 26}},
		{version: "invalid", want: [3]int{-1, -1, -1}},
	}
	for _, test := range tests {
		if got := parseServerVersion(test.version); got != test.want {
			t.Errorf("parseServerVersion(%q) = %v; want %v", test.version, got, test.want)
		}
	}
}

func TestQueryAttributesLive(t *testing.T) {
	if !available {
		t.Skipf("MySQL server not running on %s", netAddr)
	}

	db, err := sql.Open(driverNameTest, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	})

	var versionString string
	if err := conn.QueryRowContext(ctx, "SELECT VERSION()").Scan(&versionString); err != nil {
		t.Fatal(err)
	}
	version := parseServerVersion(versionString)
	if version[0] < 8 || version[0] == 8 && version[1] == 0 && version[2] < 26 {
		t.Skipf("query attributes in prepared statements require MySQL 8.0.26 or newer; got %s", versionString)
	}

	_, err = conn.ExecContext(ctx, "CREATE TEMPORARY TABLE query_attributes_live (attribute_value VARCHAR(255), bound_value VARCHAR(255))")
	if err != nil {
		t.Fatal(err)
	}

	_, err = conn.ExecContext(
		ctx,
		"INSERT INTO query_attributes_live VALUES (mysql_query_attribute_string('trace_id'), 'direct')",
		QueryAttribute{Name: "trace_id", Value: "exec-direct"},
	)
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := conn.PrepareContext(ctx, "INSERT INTO query_attributes_live VALUES (mysql_query_attribute_string('trace_id'), ?)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := stmt.Close(); err != nil {
			t.Error(err)
		}
	})
	_, err = stmt.ExecContext(ctx, QueryAttribute{Name: "trace_id", Value: "exec-prepared"}, "bound")
	if err != nil {
		t.Fatal(err)
	}

	rows, err := conn.QueryContext(ctx, "SELECT attribute_value, bound_value FROM query_attributes_live ORDER BY bound_value")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var values [][2]string
	for rows.Next() {
		var value [2]string
		if err := rows.Scan(&value[0], &value[1]); err != nil {
			t.Fatal(err)
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want := [][2]string{{"exec-prepared", "bound"}, {"exec-direct", "direct"}}
	if !slices.Equal(values, want) {
		t.Fatalf("values = %#v; want %#v", values, want)
	}

	var attribute sql.NullString
	if err := conn.QueryRowContext(ctx, "SELECT mysql_query_attribute_string('trace_id')").Scan(&attribute); err != nil {
		t.Fatal(err)
	}
	if attribute.Valid {
		t.Fatal("query attribute persisted to the next query")
	}
}

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
	"reflect"
	"testing"
)

func checkNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func checkEqual[T any](t *testing.T, want, got T) {
	t.Helper()
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("want %#v, got %#v", want, got)
	}
}

func TestNamedValueToValueQueryAttributes(t *testing.T) {
	args, attrs, err := namedValueToValue([]driver.NamedValue{
		{Ordinal: 1, Value: int64(42)},
		{Ordinal: 2, Value: QueryAttribute{Name: "trace", Value: "abc"}},
		{Ordinal: 3, Value: "bound"},
		{Ordinal: 4, Value: QueryAttribute{Name: "region", Value: "west"}},
	})
	checkNoError(t, err)
	checkEqual(t, []driver.Value{int64(42), "bound"}, args)
	checkEqual(t, []QueryAttribute{
		{Name: "trace", Value: "abc"},
		{Name: "region", Value: "west"},
	}, attrs)
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
	checkNoError(t, err)

	if len(conn.written) != int(conn.written[0])+4 {
		t.Fatalf("packet length header is %d, packet length is %d", conn.written[0], len(conn.written)-4)
	}
	checkEqual(t, []byte{
		comQuery,
		1, 1, // parameter count and parameter set count
		0, // null bitmap
		1, // new parameters bound
		byte(fieldTypeString), 0, 5, 't', 'r', 'a', 'c', 'e',
		3, 'a', 'b', 'c',
		'S', 'E', 'L', 'E', 'C', 'T', ' ', '1',
	}, conn.written[4:])
}

func TestWriteQueryPacketWithoutAttributes(t *testing.T) {
	conn, mc := newRWMockConn(0)
	mc.capabilities = clientQueryAttributes

	err := mc.writeQueryPacket("DO 1", nil)
	checkNoError(t, err)
	checkEqual(t, []byte{comQuery, 0, 1, 'D', 'O', ' ', '1'}, conn.written[4:])
}

func TestWriteQueryPacketAttributesDoNotPersist(t *testing.T) {
	conn, mc := newRWMockConn(0)
	mc.capabilities = clientQueryAttributes

	checkNoError(t, mc.writeQueryPacket("DO 1", []QueryAttribute{{Name: "trace", Value: "abc"}}))
	conn.written = nil
	checkNoError(t, mc.writeQueryPacket("DO 2", nil))

	checkEqual(t, []byte{comQuery, 0, 1, 'D', 'O', ' ', '2'}, conn.written[4:])
}

func TestWriteQueryPacketIgnoresAttributesWithoutCapability(t *testing.T) {
	conn, mc := newRWMockConn(0)

	err := mc.writeQueryPacket("DO 1", []QueryAttribute{{Name: "trace", Value: "abc"}})
	checkNoError(t, err)
	checkEqual(t, []byte{comQuery, 'D', 'O', ' ', '1'}, conn.written[4:])
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
	checkNoError(t, err)

	checkEqual(t, []byte{
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
	}, conn.written[4:])
}

func TestWriteExecutePacketIgnoresUnsafeQueryAttribute(t *testing.T) {
	conn, mc := newRWMockConn(0)
	mc.capabilities = clientQueryAttributes
	mc.serverVersion = [3]int{8, 0, 25}
	stmt := mysqlStmt{mc: mc, id: 7}

	err := stmt.writeExecutePacket(nil, []QueryAttribute{{Name: "trace", Value: "abc"}})
	checkNoError(t, err)
	checkEqual(t, []byte{
		comStmtExecute,
		7, 0, 0, 0,
		0,
		1, 0, 0, 0,
	}, conn.written[4:])
}

func TestParseServerVersion(t *testing.T) {
	checkEqual(t, [3]int{8, 0, 26}, parseServerVersion("8.0.26-commercial"))
	checkEqual(t, [3]int{-1, -1, -1}, parseServerVersion("invalid"))
}

func TestQueryAttributesLive(t *testing.T) {
	if !available {
		t.Skipf("MySQL server not running on %s", netAddr)
	}

	db, err := sql.Open(driverNameTest, dsn)
	checkNoError(t, err)
	t.Cleanup(func() { checkNoError(t, db.Close()) })

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	checkNoError(t, err)
	t.Cleanup(func() { checkNoError(t, conn.Close()) })

	var versionString string
	checkNoError(t, conn.QueryRowContext(ctx, "SELECT VERSION()").Scan(&versionString))
	version := parseServerVersion(versionString)
	if version[0] < 8 || version[0] == 8 && version[1] == 0 && version[2] < 26 {
		t.Skipf("query attributes in prepared statements require MySQL 8.0.26 or newer; got %s", versionString)
	}

	_, err = conn.ExecContext(ctx, "CREATE TEMPORARY TABLE query_attributes_live (attribute_value VARCHAR(255), bound_value VARCHAR(255))")
	checkNoError(t, err)

	_, err = conn.ExecContext(
		ctx,
		"INSERT INTO query_attributes_live VALUES (mysql_query_attribute_string('trace_id'), 'direct')",
		QueryAttribute{Name: "trace_id", Value: "exec-direct"},
	)
	checkNoError(t, err)

	stmt, err := conn.PrepareContext(ctx, "INSERT INTO query_attributes_live VALUES (mysql_query_attribute_string('trace_id'), ?)")
	checkNoError(t, err)
	t.Cleanup(func() { checkNoError(t, stmt.Close()) })
	_, err = stmt.ExecContext(ctx, QueryAttribute{Name: "trace_id", Value: "exec-prepared"}, "bound")
	checkNoError(t, err)

	rows, err := conn.QueryContext(ctx, "SELECT attribute_value, bound_value FROM query_attributes_live ORDER BY bound_value")
	checkNoError(t, err)
	defer rows.Close()

	var values [][2]string
	for rows.Next() {
		var value [2]string
		checkNoError(t, rows.Scan(&value[0], &value[1]))
		values = append(values, value)
	}
	checkNoError(t, rows.Err())
	checkEqual(t, [][2]string{{"exec-prepared", "bound"}, {"exec-direct", "direct"}}, values)

	var attribute sql.NullString
	checkNoError(t, conn.QueryRowContext(ctx, "SELECT mysql_query_attribute_string('trace_id')").Scan(&attribute))
	if attribute.Valid {
		t.Fatal("query attribute persisted to the next query")
	}
}

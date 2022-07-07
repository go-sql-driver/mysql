// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2013 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"bytes"
	"context"
	"crypto/tls"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net"
	"net/url"
	"os"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Ensure that all the driver interfaces are implemented
var (
	_ driver.Rows = &binaryRows{}
	_ driver.Rows = &textRows{}
)

var (
	user      string
	pass      string
	prot      string
	addr      string
	dbname    string
	dsn       string
	netAddr   string
	available bool
)

var (
	tDate      = time.Date(2012, 6, 14, 0, 0, 0, 0, time.UTC)
	sDate      = "2012-06-14"
	tDateTime  = time.Date(2011, 11, 20, 21, 27, 37, 0, time.UTC)
	sDateTime  = "2011-11-20 21:27:37"
	tDate0     = time.Time{}
	sDate0     = "0000-00-00"
	sDateTime0 = "0000-00-00 00:00:00"
)

// See https://github.com/go-sql-driver/mysql/wiki/Testing
func init() {
	// get environment variables
	env := func(key, defaultValue string) string {
		if value := os.Getenv(key); value != "" {
			return value
		}
		return defaultValue
	}
	user = env("MYSQL_TEST_USER", "root")
	pass = env("MYSQL_TEST_PASS", "")
	prot = env("MYSQL_TEST_PROT", "tcp")
	addr = env("MYSQL_TEST_ADDR", "localhost:3306")
	dbname = env("MYSQL_TEST_DBNAME", "gotest")
	netAddr = fmt.Sprintf("%s(%s)", prot, addr)
	dsn = fmt.Sprintf("%s:%s@%s/%s?timeout=30s", user, pass, netAddr, dbname)
	c, err := net.Dial(prot, addr)
	if err == nil {
		available = true
		c.Close()
	}
}

type DBTest struct {
	*testing.T
	db *sql.DB
}

type netErrorMock struct {
	temporary bool
	timeout   bool
}

func (e netErrorMock) Temporary() bool {
	return e.temporary
}

func (e netErrorMock) Timeout() bool {
	return e.timeout
}

func (e netErrorMock) Error() string {
	return fmt.Sprintf("mock net error. Temporary: %v, Timeout %v", e.temporary, e.timeout)
}

func runTestsWithMultiStatement(t *testing.T, dsn string, tests ...func(dbt *DBTest)) {
	if !available {
		t.Skipf("MySQL server not running on %s", netAddr)
	}

	dsn += "&multiStatements=true"
	var db *sql.DB
	if _, err := ParseDSN(dsn); err != errInvalidDSNUnsafeCollation {
		db, err = sql.Open("mysql", dsn)
		if err != nil {
			t.Fatalf("error connecting: %s", err.Error())
		}
		defer db.Close()
	}

	dbt := &DBTest{t, db}
	for _, test := range tests {
		test(dbt)
		dbt.db.Exec("DROP TABLE IF EXISTS test")
	}
}

func runTests(t *testing.T, dsn string, tests ...func(dbt *DBTest)) {
	if !available {
		t.Skipf("MySQL server not running on %s", netAddr)
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS test")

	dsn2 := dsn + "&interpolateParams=true"
	var db2 *sql.DB
	if _, err := ParseDSN(dsn2); err != errInvalidDSNUnsafeCollation {
		db2, err = sql.Open("mysql", dsn2)
		if err != nil {
			t.Fatalf("error connecting: %s", err.Error())
		}
		defer db2.Close()
	}

	dsn3 := dsn + "&multiStatements=true"
	var db3 *sql.DB
	if _, err := ParseDSN(dsn3); err != errInvalidDSNUnsafeCollation {
		db3, err = sql.Open("mysql", dsn3)
		if err != nil {
			t.Fatalf("error connecting: %s", err.Error())
		}
		defer db3.Close()
	}

	dbt := &DBTest{t, db}
	dbt2 := &DBTest{t, db2}
	dbt3 := &DBTest{t, db3}
	for _, test := range tests {
		test(dbt)
		dbt.db.Exec("DROP TABLE IF EXISTS test")
		if db2 != nil {
			test(dbt2)
			dbt2.db.Exec("DROP TABLE IF EXISTS test")
		}
		if db3 != nil {
			test(dbt3)
			dbt3.db.Exec("DROP TABLE IF EXISTS test")
		}
	}
}

func (dbt *DBTest) fail(method, query string, err error) {
	if len(query) > 300 {
		query = "[query too large to print]"
	}
	dbt.Fatalf("error on %s %s: %s", method, query, err.Error())
}

func (dbt *DBTest) mustExec(query string, args ...interface{}) (res sql.Result) {
	res, err := dbt.db.Exec(query, args...)
	if err != nil {
		dbt.fail("exec", query, err)
	}
	return res
}

func (dbt *DBTest) mustQuery(query string, args ...interface{}) (rows *sql.Rows) {
	rows, err := dbt.db.Query(query, args...)
	if err != nil {
		dbt.fail("query", query, err)
	}
	return rows
}

func maybeSkip(t *testing.T, err error, skipErrno uint16) {
	mySQLErr, ok := err.(*MySQLError)
	if !ok {
		return
	}

	if mySQLErr.Number == skipErrno {
		t.Skipf("skipping test for error: %v", err)
	}
}

func TestEmptyQuery(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		// just a comment, no query
		rows := dbt.mustQuery("--")
		defer rows.Close()
		// will hang before #255
		if rows.Next() {
			dbt.Errorf("next on rows must be false")
		}
	})
}

func TestCRUD(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		// Create Table
		dbt.mustExec("CREATE TABLE test (value BOOL)")

		// Test for unexpected data
		var out bool
		rows := dbt.mustQuery("SELECT * FROM test")
		if rows.Next() {
			dbt.Error("unexpected data in empty table")
		}
		rows.Close()

		// Create Data
		res := dbt.mustExec("INSERT INTO test VALUES (1)")
		count, err := res.RowsAffected()
		if err != nil {
			dbt.Fatalf("res.RowsAffected() returned error: %s", err.Error())
		}
		if count != 1 {
			dbt.Fatalf("expected 1 affected row, got %d", count)
		}

		id, err := res.LastInsertId()
		if err != nil {
			dbt.Fatalf("res.LastInsertId() returned error: %s", err.Error())
		}
		if id != 0 {
			dbt.Fatalf("expected InsertId 0, got %d", id)
		}

		// Read
		rows = dbt.mustQuery("SELECT value FROM test")
		if rows.Next() {
			rows.Scan(&out)
			if true != out {
				dbt.Errorf("true != %t", out)
			}

			if rows.Next() {
				dbt.Error("unexpected data")
			}
		} else {
			dbt.Error("no data")
		}
		rows.Close()

		// Update
		res = dbt.mustExec("UPDATE test SET value = ? WHERE value = ?", false, true)
		count, err = res.RowsAffected()
		if err != nil {
			dbt.Fatalf("res.RowsAffected() returned error: %s", err.Error())
		}
		if count != 1 {
			dbt.Fatalf("expected 1 affected row, got %d", count)
		}

		// Check Update
		rows = dbt.mustQuery("SELECT value FROM test")
		if rows.Next() {
			rows.Scan(&out)
			if false != out {
				dbt.Errorf("false != %t", out)
			}

			if rows.Next() {
				dbt.Error("unexpected data")
			}
		} else {
			dbt.Error("no data")
		}
		rows.Close()

		// Delete
		res = dbt.mustExec("DELETE FROM test WHERE value = ?", false)
		count, err = res.RowsAffected()
		if err != nil {
			dbt.Fatalf("res.RowsAffected() returned error: %s", err.Error())
		}
		if count != 1 {
			dbt.Fatalf("expected 1 affected row, got %d", count)
		}

		// Check for unexpected rows
		res = dbt.mustExec("DELETE FROM test")
		count, err = res.RowsAffected()
		if err != nil {
			dbt.Fatalf("res.RowsAffected() returned error: %s", err.Error())
		}
		if count != 0 {
			dbt.Fatalf("expected 0 affected row, got %d", count)
		}
	})
}

func TestMultiQuery(t *testing.T) {
	runTestsWithMultiStatement(t, dsn, func(dbt *DBTest) {
		// Create Table
		dbt.mustExec("CREATE TABLE `test` (`id` int(11) NOT NULL, `value` int(11) NOT NULL) ")

		// Create Data
		res := dbt.mustExec("INSERT INTO test VALUES (1, 1)")
		count, err := res.RowsAffected()
		if err != nil {
			dbt.Fatalf("res.RowsAffected() returned error: %s", err.Error())
		}
		if count != 1 {
			dbt.Fatalf("expected 1 affected row, got %d", count)
		}

		// Update
		res = dbt.mustExec("UPDATE test SET value = 3 WHERE id = 1; UPDATE test SET value = 4 WHERE id = 1; UPDATE test SET value = 5 WHERE id = 1;")
		count, err = res.RowsAffected()
		if err != nil {
			dbt.Fatalf("res.RowsAffected() returned error: %s", err.Error())
		}
		if count != 1 {
			dbt.Fatalf("expected 1 affected row, got %d", count)
		}

		// Read
		var out int
		rows := dbt.mustQuery("SELECT value FROM test WHERE id=1;")
		if rows.Next() {
			rows.Scan(&out)
			if 5 != out {
				dbt.Errorf("5 != %d", out)
			}

			if rows.Next() {
				dbt.Error("unexpected data")
			}
		} else {
			dbt.Error("no data")
		}
		rows.Close()

	})
}

func TestInt(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		types := [5]string{"TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT"}
		in := int64(42)
		var out int64
		var rows *sql.Rows

		// SIGNED
		for _, v := range types {
			dbt.mustExec("CREATE TABLE test (value " + v + ")")

			dbt.mustExec("INSERT INTO test VALUES (?)", in)

			rows = dbt.mustQuery("SELECT value FROM test")
			if rows.Next() {
				rows.Scan(&out)
				if in != out {
					dbt.Errorf("%s: %d != %d", v, in, out)
				}
			} else {
				dbt.Errorf("%s: no data", v)
			}
			rows.Close()

			dbt.mustExec("DROP TABLE IF EXISTS test")
		}

		// UNSIGNED ZEROFILL
		for _, v := range types {
			dbt.mustExec("CREATE TABLE test (value " + v + " ZEROFILL)")

			dbt.mustExec("INSERT INTO test VALUES (?)", in)

			rows = dbt.mustQuery("SELECT value FROM test")
			if rows.Next() {
				rows.Scan(&out)
				if in != out {
					dbt.Errorf("%s ZEROFILL: %d != %d", v, in, out)
				}
			} else {
				dbt.Errorf("%s ZEROFILL: no data", v)
			}
			rows.Close()

			dbt.mustExec("DROP TABLE IF EXISTS test")
		}
	})
}

func TestFloat32(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		types := [2]string{"FLOAT", "DOUBLE"}
		in := float32(42.23)
		var out float32
		var rows *sql.Rows
		for _, v := range types {
			dbt.mustExec("CREATE TABLE test (value " + v + ")")
			dbt.mustExec("INSERT INTO test VALUES (?)", in)
			rows = dbt.mustQuery("SELECT value FROM test")
			if rows.Next() {
				rows.Scan(&out)
				if in != out {
					dbt.Errorf("%s: %g != %g", v, in, out)
				}
			} else {
				dbt.Errorf("%s: no data", v)
			}
			rows.Close()
			dbt.mustExec("DROP TABLE IF EXISTS test")
		}
	})
}

func TestFloat64(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		types := [2]string{"FLOAT", "DOUBLE"}
		var expected float64 = 42.23
		var out float64
		var rows *sql.Rows
		for _, v := range types {
			dbt.mustExec("CREATE TABLE test (value " + v + ")")
			dbt.mustExec("INSERT INTO test VALUES (42.23)")
			rows = dbt.mustQuery("SELECT value FROM test")
			if rows.Next() {
				rows.Scan(&out)
				if expected != out {
					dbt.Errorf("%s: %g != %g", v, expected, out)
				}
			} else {
				dbt.Errorf("%s: no data", v)
			}
			rows.Close()
			dbt.mustExec("DROP TABLE IF EXISTS test")
		}
	})
}

func TestFloat64Placeholder(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		types := [2]string{"FLOAT", "DOUBLE"}
		var expected float64 = 42.23
		var out float64
		var rows *sql.Rows
		for _, v := range types {
			dbt.mustExec("CREATE TABLE test (id int, value " + v + ")")
			dbt.mustExec("INSERT INTO test VALUES (1, 42.23)")
			rows = dbt.mustQuery("SELECT value FROM test WHERE id = ?", 1)
			if rows.Next() {
				rows.Scan(&out)
				if expected != out {
					dbt.Errorf("%s: %g != %g", v, expected, out)
				}
			} else {
				dbt.Errorf("%s: no data", v)
			}
			rows.Close()
			dbt.mustExec("DROP TABLE IF EXISTS test")
		}
	})
}

func TestString(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		types := [6]string{"CHAR(255)", "VARCHAR(255)", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT"}
		in := "κόσμε üöäßñóùéàâÿœ'îë Árvíztűrő いろはにほへとちりぬるを イロハニホヘト דג סקרן чащах  น่าฟังเอย"
		var out string
		var rows *sql.Rows

		for _, v := range types {
			dbt.mustExec("CREATE TABLE test (value " + v + ") CHARACTER SET utf8")

			dbt.mustExec("INSERT INTO test VALUES (?)", in)

			rows = dbt.mustQuery("SELECT value FROM test")
			if rows.Next() {
				rows.Scan(&out)
				if in != out {
					dbt.Errorf("%s: %s != %s", v, in, out)
				}
			} else {
				dbt.Errorf("%s: no data", v)
			}
			rows.Close()

			dbt.mustExec("DROP TABLE IF EXISTS test")
		}

		// BLOB
		dbt.mustExec("CREATE TABLE test (id int, value BLOB) CHARACTER SET utf8")

		id := 2
		in = "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, " +
			"sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, " +
			"sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. " +
			"Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. " +
			"Lorem ipsum dolor sit amet, consetetur sadipscing elitr, " +
			"sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, " +
			"sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. " +
			"Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet."
		dbt.mustExec("INSERT INTO test VALUES (?, ?)", id, in)

		err := dbt.db.QueryRow("SELECT value FROM test WHERE id = ?", id).Scan(&out)
		if err != nil {
			dbt.Fatalf("Error on BLOB-Query: %s", err.Error())
		} else if out != in {
			dbt.Errorf("BLOB: %s != %s", in, out)
		}
	})
}

func TestRawBytes(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		v1 := []byte("aaa")
		v2 := []byte("bbb")
		rows := dbt.mustQuery("SELECT ?, ?", v1, v2)
		defer rows.Close()
		if rows.Next() {
			var o1, o2 sql.RawBytes
			if err := rows.Scan(&o1, &o2); err != nil {
				dbt.Errorf("Got error: %v", err)
			}
			if !bytes.Equal(v1, o1) {
				dbt.Errorf("expected %v, got %v", v1, o1)
			}
			if !bytes.Equal(v2, o2) {
				dbt.Errorf("expected %v, got %v", v2, o2)
			}
			// https://github.com/go-sql-driver/mysql/issues/765
			// Appending to RawBytes shouldn't overwrite next RawBytes.
			o1 = append(o1, "xyzzy"...)
			if !bytes.Equal(v2, o2) {
				dbt.Errorf("expected %v, got %v", v2, o2)
			}
		} else {
			dbt.Errorf("no data")
		}
	})
}

func TestRawMessage(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		v1 := json.RawMessage("{}")
		v2 := json.RawMessage("[]")
		rows := dbt.mustQuery("SELECT ?, ?", v1, v2)
		defer rows.Close()
		if rows.Next() {
			var o1, o2 json.RawMessage
			if err := rows.Scan(&o1, &o2); err != nil {
				dbt.Errorf("Got error: %v", err)
			}
			if !bytes.Equal(v1, o1) {
				dbt.Errorf("expected %v, got %v", v1, o1)
			}
			if !bytes.Equal(v2, o2) {
				dbt.Errorf("expected %v, got %v", v2, o2)
			}
		} else {
			dbt.Errorf("no data")
		}
	})
}

type testValuer struct {
	value string
}

func (tv testValuer) Value() (driver.Value, error) {
	return tv.value, nil
}

func TestValuer(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		in := testValuer{"a_value"}
		var out string
		var rows *sql.Rows

		dbt.mustExec("CREATE TABLE test (value VARCHAR(255)) CHARACTER SET utf8")
		dbt.mustExec("INSERT INTO test VALUES (?)", in)
		rows = dbt.mustQuery("SELECT value FROM test")
		if rows.Next() {
			rows.Scan(&out)
			if in.value != out {
				dbt.Errorf("Valuer: %v != %s", in, out)
			}
		} else {
			dbt.Errorf("Valuer: no data")
		}
		rows.Close()

		dbt.mustExec("DROP TABLE IF EXISTS test")
	})
}

type testValuerWithValidation struct {
	value string
}

func (tv testValuerWithValidation) Value() (driver.Value, error) {
	if len(tv.value) == 0 {
		return nil, fmt.Errorf("Invalid string valuer. Value must not be empty")
	}

	return tv.value, nil
}

func TestValuerWithValidation(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		in := testValuerWithValidation{"a_value"}
		var out string
		var rows *sql.Rows

		dbt.mustExec("CREATE TABLE testValuer (value VARCHAR(255)) CHARACTER SET utf8")
		dbt.mustExec("INSERT INTO testValuer VALUES (?)", in)

		rows = dbt.mustQuery("SELECT value FROM testValuer")
		defer rows.Close()

		if rows.Next() {
			rows.Scan(&out)
			if in.value != out {
				dbt.Errorf("Valuer: %v != %s", in, out)
			}
		} else {
			dbt.Errorf("Valuer: no data")
		}

		if _, err := dbt.db.Exec("INSERT INTO testValuer VALUES (?)", testValuerWithValidation{""}); err == nil {
			dbt.Errorf("Failed to check valuer error")
		}

		if _, err := dbt.db.Exec("INSERT INTO testValuer VALUES (?)", nil); err != nil {
			dbt.Errorf("Failed to check nil")
		}

		if _, err := dbt.db.Exec("INSERT INTO testValuer VALUES (?)", map[string]bool{}); err == nil {
			dbt.Errorf("Failed to check not valuer")
		}

		dbt.mustExec("DROP TABLE IF EXISTS testValuer")
	})
}

type timeTests struct {
	dbtype  string
	tlayout string
	tests   []timeTest
}

type timeTest struct {
	s string // leading "!": do not use t as value in queries
	t time.Time
}

type timeMode byte

func (t timeMode) String() string {
	switch t {
	case binaryString:
		return "binary:string"
	case binaryTime:
		return "binary:time.Time"
	case textString:
		return "text:string"
	}
	panic("unsupported timeMode")
}

func (t timeMode) Binary() bool {
	switch t {
	case binaryString, binaryTime:
		return true
	}
	return false
}

const (
	binaryString timeMode = iota
	binaryTime
	textString
)

func (t timeTest) genQuery(dbtype string, mode timeMode) string {
	var inner string
	if mode.Binary() {
		inner = "?"
	} else {
		inner = `"%s"`
	}
	return `SELECT cast(` + inner + ` as ` + dbtype + `)`
}

func (t timeTest) run(dbt *DBTest, dbtype, tlayout string, mode timeMode) {
	var rows *sql.Rows
	query := t.genQuery(dbtype, mode)
	switch mode {
	case binaryString:
		rows = dbt.mustQuery(query, t.s)
	case binaryTime:
		rows = dbt.mustQuery(query, t.t)
	case textString:
		query = fmt.Sprintf(query, t.s)
		rows = dbt.mustQuery(query)
	default:
		panic("unsupported mode")
	}
	defer rows.Close()
	var err error
	if !rows.Next() {
		err = rows.Err()
		if err == nil {
			err = fmt.Errorf("no data")
		}
		dbt.Errorf("%s [%s]: %s", dbtype, mode, err)
		return
	}
	var dst interface{}
	err = rows.Scan(&dst)
	if err != nil {
		dbt.Errorf("%s [%s]: %s", dbtype, mode, err)
		return
	}
	switch val := dst.(type) {
	case []uint8:
		str := string(val)
		if str == t.s {
			return
		}
		if mode.Binary() && dbtype == "DATETIME" && len(str) == 26 && str[:19] == t.s {
			// a fix mainly for TravisCI:
			// accept full microsecond resolution in result for DATETIME columns
			// where the binary protocol was used
			return
		}
		dbt.Errorf("%s [%s] to string: expected %q, got %q",
			dbtype, mode,
			t.s, str,
		)
	case time.Time:
		if val == t.t {
			return
		}
		dbt.Errorf("%s [%s] to string: expected %q, got %q",
			dbtype, mode,
			t.s, val.Format(tlayout),
		)
	default:
		fmt.Printf("%#v\n", []interface{}{dbtype, tlayout, mode, t.s, t.t})
		dbt.Errorf("%s [%s]: unhandled type %T (is '%v')",
			dbtype, mode,
			val, val,
		)
	}
}

func TestDateTime(t *testing.T) {
	afterTime := func(t time.Time, d string) time.Time {
		dur, err := time.ParseDuration(d)
		if err != nil {
			panic(err)
		}
		return t.Add(dur)
	}
	// NOTE: MySQL rounds DATETIME(x) up - but that's not included in the tests
	format := "2006-01-02 15:04:05.999999"
	t0 := time.Time{}
	tstr0 := "0000-00-00 00:00:00.000000"
	testcases := []timeTests{
		{"DATE", format[:10], []timeTest{
			{t: time.Date(2011, 11, 20, 0, 0, 0, 0, time.UTC)},
			{t: t0, s: tstr0[:10]},
		}},
		{"DATETIME", format[:19], []timeTest{
			{t: time.Date(2011, 11, 20, 21, 27, 37, 0, time.UTC)},
			{t: t0, s: tstr0[:19]},
		}},
		{"DATETIME(0)", format[:21], []timeTest{
			{t: time.Date(2011, 11, 20, 21, 27, 37, 0, time.UTC)},
			{t: t0, s: tstr0[:19]},
		}},
		{"DATETIME(1)", format[:21], []timeTest{
			{t: time.Date(2011, 11, 20, 21, 27, 37, 100000000, time.UTC)},
			{t: t0, s: tstr0[:21]},
		}},
		{"DATETIME(6)", format, []timeTest{
			{t: time.Date(2011, 11, 20, 21, 27, 37, 123456000, time.UTC)},
			{t: t0, s: tstr0},
		}},
		{"TIME", format[11:19], []timeTest{
			{t: afterTime(t0, "12345s")},
			{s: "!-12:34:56"},
			{s: "!-838:59:59"},
			{s: "!838:59:59"},
			{t: t0, s: tstr0[11:19]},
		}},
		{"TIME(0)", format[11:19], []timeTest{
			{t: afterTime(t0, "12345s")},
			{s: "!-12:34:56"},
			{s: "!-838:59:59"},
			{s: "!838:59:59"},
			{t: t0, s: tstr0[11:19]},
		}},
		{"TIME(1)", format[11:21], []timeTest{
			{t: afterTime(t0, "12345600ms")},
			{s: "!-12:34:56.7"},
			{s: "!-838:59:58.9"},
			{s: "!838:59:58.9"},
			{t: t0, s: tstr0[11:21]},
		}},
		{"TIME(6)", format[11:], []timeTest{
			{t: afterTime(t0, "1234567890123000ns")},
			{s: "!-12:34:56.789012"},
			{s: "!-838:59:58.999999"},
			{s: "!838:59:58.999999"},
			{t: t0, s: tstr0[11:]},
		}},
	}
	dsns := []string{
		dsn + "&parseTime=true",
		dsn + "&parseTime=false",
	}
	for _, testdsn := range dsns {
		runTests(t, testdsn, func(dbt *DBTest) {
			microsecsSupported := false
			zeroDateSupported := false
			var rows *sql.Rows
			var err error
			rows, err = dbt.db.Query(`SELECT cast("00:00:00.1" as TIME(1)) = "00:00:00.1"`)
			if err == nil {
				rows.Scan(&microsecsSupported)
				rows.Close()
			}
			rows, err = dbt.db.Query(`SELECT cast("0000-00-00" as DATE) = "0000-00-00"`)
			if err == nil {
				rows.Scan(&zeroDateSupported)
				rows.Close()
			}
			for _, setups := range testcases {
				if t := setups.dbtype; !microsecsSupported && t[len(t)-1:] == ")" {
					// skip fractional second tests if unsupported by server
					continue
				}
				for _, setup := range setups.tests {
					allowBinTime := true
					if setup.s == "" {
						// fill time string wherever Go can reliable produce it
						setup.s = setup.t.Format(setups.tlayout)
					} else if setup.s[0] == '!' {
						// skip tests using setup.t as source in queries
						allowBinTime = false
						// fix setup.s - remove the "!"
						setup.s = setup.s[1:]
					}
					if !zeroDateSupported && setup.s == tstr0[:len(setup.s)] {
						// skip disallowed 0000-00-00 date
						continue
					}
					setup.run(dbt, setups.dbtype, setups.tlayout, textString)
					setup.run(dbt, setups.dbtype, setups.tlayout, binaryString)
					if allowBinTime {
						setup.run(dbt, setups.dbtype, setups.tlayout, binaryTime)
					}
				}
			}
		})
	}
}

func TestTimestampMicros(t *testing.T) {
	format := "2006-01-02 15:04:05.999999"
	f0 := format[:19]
	f1 := format[:21]
	f6 := format[:26]
	runTests(t, dsn, func(dbt *DBTest) {
		// check if microseconds are supported.
		// Do not use timestamp(x) for that check - before 5.5.6, x would mean display width
		// and not precision.
		// Se last paragraph at http://dev.mysql.com/doc/refman/5.6/en/fractional-seconds.html
		microsecsSupported := false
		if rows, err := dbt.db.Query(`SELECT cast("00:00:00.1" as TIME(1)) = "00:00:00.1"`); err == nil {
			rows.Scan(&microsecsSupported)
			rows.Close()
		}
		if !microsecsSupported {
			// skip test
			return
		}
		_, err := dbt.db.Exec(`
			CREATE TABLE test (
				value0 TIMESTAMP NOT NULL DEFAULT '` + f0 + `',
				value1 TIMESTAMP(1) NOT NULL DEFAULT '` + f1 + `',
				value6 TIMESTAMP(6) NOT NULL DEFAULT '` + f6 + `'
			)`,
		)
		if err != nil {
			dbt.Error(err)
		}
		defer dbt.mustExec("DROP TABLE IF EXISTS test")
		dbt.mustExec("INSERT INTO test SET value0=?, value1=?, value6=?", f0, f1, f6)
		var res0, res1, res6 string
		rows := dbt.mustQuery("SELECT * FROM test")
		defer rows.Close()
		if !rows.Next() {
			dbt.Errorf("test contained no selectable values")
		}
		err = rows.Scan(&res0, &res1, &res6)
		if err != nil {
			dbt.Error(err)
		}
		if res0 != f0 {
			dbt.Errorf("expected %q, got %q", f0, res0)
		}
		if res1 != f1 {
			dbt.Errorf("expected %q, got %q", f1, res1)
		}
		if res6 != f6 {
			dbt.Errorf("expected %q, got %q", f6, res6)
		}
	})
}

func TestNULL(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		nullStmt, err := dbt.db.Prepare("SELECT NULL")
		if err != nil {
			dbt.Fatal(err)
		}
		defer nullStmt.Close()

		nonNullStmt, err := dbt.db.Prepare("SELECT 1")
		if err != nil {
			dbt.Fatal(err)
		}
		defer nonNullStmt.Close()

		// NullBool
		var nb sql.NullBool
		// Invalid
		if err = nullStmt.QueryRow().Scan(&nb); err != nil {
			dbt.Fatal(err)
		}
		if nb.Valid {
			dbt.Error("valid NullBool which should be invalid")
		}
		// Valid
		if err = nonNullStmt.QueryRow().Scan(&nb); err != nil {
			dbt.Fatal(err)
		}
		if !nb.Valid {
			dbt.Error("invalid NullBool which should be valid")
		} else if nb.Bool != true {
			dbt.Errorf("Unexpected NullBool value: %t (should be true)", nb.Bool)
		}

		// NullFloat64
		var nf sql.NullFloat64
		// Invalid
		if err = nullStmt.QueryRow().Scan(&nf); err != nil {
			dbt.Fatal(err)
		}
		if nf.Valid {
			dbt.Error("valid NullFloat64 which should be invalid")
		}
		// Valid
		if err = nonNullStmt.QueryRow().Scan(&nf); err != nil {
			dbt.Fatal(err)
		}
		if !nf.Valid {
			dbt.Error("invalid NullFloat64 which should be valid")
		} else if nf.Float64 != float64(1) {
			dbt.Errorf("unexpected NullFloat64 value: %f (should be 1.0)", nf.Float64)
		}

		// NullInt64
		var ni sql.NullInt64
		// Invalid
		if err = nullStmt.QueryRow().Scan(&ni); err != nil {
			dbt.Fatal(err)
		}
		if ni.Valid {
			dbt.Error("valid NullInt64 which should be invalid")
		}
		// Valid
		if err = nonNullStmt.QueryRow().Scan(&ni); err != nil {
			dbt.Fatal(err)
		}
		if !ni.Valid {
			dbt.Error("invalid NullInt64 which should be valid")
		} else if ni.Int64 != int64(1) {
			dbt.Errorf("unexpected NullInt64 value: %d (should be 1)", ni.Int64)
		}

		// NullString
		var ns sql.NullString
		// Invalid
		if err = nullStmt.QueryRow().Scan(&ns); err != nil {
			dbt.Fatal(err)
		}
		if ns.Valid {
			dbt.Error("valid NullString which should be invalid")
		}
		// Valid
		if err = nonNullStmt.QueryRow().Scan(&ns); err != nil {
			dbt.Fatal(err)
		}
		if !ns.Valid {
			dbt.Error("invalid NullString which should be valid")
		} else if ns.String != `1` {
			dbt.Error("unexpected NullString value:" + ns.String + " (should be `1`)")
		}

		// nil-bytes
		var b []byte
		// Read nil
		if err = nullStmt.QueryRow().Scan(&b); err != nil {
			dbt.Fatal(err)
		}
		if b != nil {
			dbt.Error("non-nil []byte which should be nil")
		}
		// Read non-nil
		if err = nonNullStmt.QueryRow().Scan(&b); err != nil {
			dbt.Fatal(err)
		}
		if b == nil {
			dbt.Error("nil []byte which should be non-nil")
		}
		// Insert nil
		b = nil
		success := false
		if err = dbt.db.QueryRow("SELECT ? IS NULL", b).Scan(&success); err != nil {
			dbt.Fatal(err)
		}
		if !success {
			dbt.Error("inserting []byte(nil) as NULL failed")
		}
		// Check input==output with input==nil
		b = nil
		if err = dbt.db.QueryRow("SELECT ?", b).Scan(&b); err != nil {
			dbt.Fatal(err)
		}
		if b != nil {
			dbt.Error("non-nil echo from nil input")
		}
		// Check input==output with input!=nil
		b = []byte("")
		if err = dbt.db.QueryRow("SELECT ?", b).Scan(&b); err != nil {
			dbt.Fatal(err)
		}
		if b == nil {
			dbt.Error("nil echo from non-nil input")
		}

		// Insert NULL
		dbt.mustExec("CREATE TABLE test (dummmy1 int, value int, dummy2 int)")

		dbt.mustExec("INSERT INTO test VALUES (?, ?, ?)", 1, nil, 2)

		var out interface{}
		rows := dbt.mustQuery("SELECT * FROM test")
		defer rows.Close()
		if rows.Next() {
			rows.Scan(&out)
			if out != nil {
				dbt.Errorf("%v != nil", out)
			}
		} else {
			dbt.Error("no data")
		}
	})
}

func TestUint64(t *testing.T) {
	const (
		u0    = uint64(0)
		uall  = ^u0
		uhigh = uall >> 1
		utop  = ^uhigh
		s0    = int64(0)
		sall  = ^s0
		shigh = int64(uhigh)
		stop  = ^shigh
	)
	runTests(t, dsn, func(dbt *DBTest) {
		stmt, err := dbt.db.Prepare(`SELECT ?, ?, ? ,?, ?, ?, ?, ?`)
		if err != nil {
			dbt.Fatal(err)
		}
		defer stmt.Close()
		row := stmt.QueryRow(
			u0, uhigh, utop, uall,
			s0, shigh, stop, sall,
		)

		var ua, ub, uc, ud uint64
		var sa, sb, sc, sd int64

		err = row.Scan(&ua, &ub, &uc, &ud, &sa, &sb, &sc, &sd)
		if err != nil {
			dbt.Fatal(err)
		}
		switch {
		case ua != u0,
			ub != uhigh,
			uc != utop,
			ud != uall,
			sa != s0,
			sb != shigh,
			sc != stop,
			sd != sall:
			dbt.Fatal("unexpected result value")
		}
	})
}

func TestLongData(t *testing.T) {
	runTests(t, dsn+"&maxAllowedPacket=0", func(dbt *DBTest) {
		var maxAllowedPacketSize int
		err := dbt.db.QueryRow("select @@max_allowed_packet").Scan(&maxAllowedPacketSize)
		if err != nil {
			dbt.Fatal(err)
		}
		maxAllowedPacketSize--

		// don't get too ambitious
		if maxAllowedPacketSize > 1<<25 {
			maxAllowedPacketSize = 1 << 25
		}

		dbt.mustExec("CREATE TABLE test (value LONGBLOB)")

		in := strings.Repeat(`a`, maxAllowedPacketSize+1)
		var out string
		var rows *sql.Rows

		// Long text data
		const nonDataQueryLen = 28 // length query w/o value
		inS := in[:maxAllowedPacketSize-nonDataQueryLen]
		dbt.mustExec("INSERT INTO test VALUES('" + inS + "')")
		rows = dbt.mustQuery("SELECT value FROM test")
		defer rows.Close()
		if rows.Next() {
			rows.Scan(&out)
			if inS != out {
				dbt.Fatalf("LONGBLOB: length in: %d, length out: %d", len(inS), len(out))
			}
			if rows.Next() {
				dbt.Error("LONGBLOB: unexpexted row")
			}
		} else {
			dbt.Fatalf("LONGBLOB: no data")
		}

		// Empty table
		dbt.mustExec("TRUNCATE TABLE test")

		// Long binary data
		dbt.mustExec("INSERT INTO test VALUES(?)", in)
		rows = dbt.mustQuery("SELECT value FROM test WHERE 1=?", 1)
		defer rows.Close()
		if rows.Next() {
			rows.Scan(&out)
			if in != out {
				dbt.Fatalf("LONGBLOB: length in: %d, length out: %d", len(in), len(out))
			}
			if rows.Next() {
				dbt.Error("LONGBLOB: unexpexted row")
			}
		} else {
			if err = rows.Err(); err != nil {
				dbt.Fatalf("LONGBLOB: no data (err: %s)", err.Error())
			} else {
				dbt.Fatal("LONGBLOB: no data (err: <nil>)")
			}
		}
	})
}

func TestLoadData(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		verifyLoadDataResult := func() {
			rows, err := dbt.db.Query("SELECT * FROM test")
			if err != nil {
				dbt.Fatal(err.Error())
			}

			i := 0
			values := [4]string{
				"a string",
				"a string containing a \t",
				"a string containing a \n",
				"a string containing both \t\n",
			}

			var id int
			var value string

			for rows.Next() {
				i++
				err = rows.Scan(&id, &value)
				if err != nil {
					dbt.Fatal(err.Error())
				}
				if i != id {
					dbt.Fatalf("%d != %d", i, id)
				}
				if values[i-1] != value {
					dbt.Fatalf("%q != %q", values[i-1], value)
				}
			}
			err = rows.Err()
			if err != nil {
				dbt.Fatal(err.Error())
			}

			if i != 4 {
				dbt.Fatalf("rows count mismatch. Got %d, want 4", i)
			}
		}

		dbt.db.Exec("DROP TABLE IF EXISTS test")
		dbt.mustExec("CREATE TABLE test (id INT NOT NULL PRIMARY KEY, value TEXT NOT NULL) CHARACTER SET utf8")

		// Local File
		file, err := ioutil.TempFile("", "gotest")
		defer os.Remove(file.Name())
		if err != nil {
			dbt.Fatal(err)
		}
		RegisterLocalFile(file.Name())

		// Try first with empty file
		dbt.mustExec(fmt.Sprintf("LOAD DATA LOCAL INFILE %q INTO TABLE test", file.Name()))
		var count int
		err = dbt.db.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
		if err != nil {
			dbt.Fatal(err.Error())
		}
		if count != 0 {
			dbt.Fatalf("unexpected row count: got %d, want 0", count)
		}

		// Then fille File with data and try to load it
		file.WriteString("1\ta string\n2\ta string containing a \\t\n3\ta string containing a \\n\n4\ta string containing both \\t\\n\n")
		file.Close()
		dbt.mustExec(fmt.Sprintf("LOAD DATA LOCAL INFILE %q INTO TABLE test", file.Name()))
		verifyLoadDataResult()

		// Try with non-existing file
		_, err = dbt.db.Exec("LOAD DATA LOCAL INFILE 'doesnotexist' INTO TABLE test")
		if err == nil {
			dbt.Fatal("load non-existent file didn't fail")
		} else if err.Error() != "local file 'doesnotexist' is not registered" {
			dbt.Fatal(err.Error())
		}

		// Empty table
		dbt.mustExec("TRUNCATE TABLE test")

		// Reader
		RegisterReaderHandler("test", func() io.Reader {
			file, err = os.Open(file.Name())
			if err != nil {
				dbt.Fatal(err)
			}
			return file
		})
		dbt.mustExec("LOAD DATA LOCAL INFILE 'Reader::test' INTO TABLE test")
		verifyLoadDataResult()
		// negative test
		_, err = dbt.db.Exec("LOAD DATA LOCAL INFILE 'Reader::doesnotexist' INTO TABLE test")
		if err == nil {
			dbt.Fatal("load non-existent Reader didn't fail")
		} else if err.Error() != "Reader 'doesnotexist' is not registered" {
			dbt.Fatal(err.Error())
		}
	})
}

func TestFoundRows(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("CREATE TABLE test (id INT NOT NULL ,data INT NOT NULL)")
		dbt.mustExec("INSERT INTO test (id, data) VALUES (0, 0),(0, 0),(1, 0),(1, 0),(1, 1)")

		res := dbt.mustExec("UPDATE test SET data = 1 WHERE id = 0")
		count, err := res.RowsAffected()
		if err != nil {
			dbt.Fatalf("res.RowsAffected() returned error: %s", err.Error())
		}
		if count != 2 {
			dbt.Fatalf("Expected 2 affected rows, got %d", count)
		}
		res = dbt.mustExec("UPDATE test SET data = 1 WHERE id = 1")
		count, err = res.RowsAffected()
		if err != nil {
			dbt.Fatalf("res.RowsAffected() returned error: %s", err.Error())
		}
		if count != 2 {
			dbt.Fatalf("Expected 2 affected rows, got %d", count)
		}
	})
	runTests(t, dsn+"&clientFoundRows=true", func(dbt *DBTest) {
		dbt.mustExec("CREATE TABLE test (id INT NOT NULL ,data INT NOT NULL)")
		dbt.mustExec("INSERT INTO test (id, data) VALUES (0, 0),(0, 0),(1, 0),(1, 0),(1, 1)")

		res := dbt.mustExec("UPDATE test SET data = 1 WHERE id = 0")
		count, err := res.RowsAffected()
		if err != nil {
			dbt.Fatalf("res.RowsAffected() returned error: %s", err.Error())
		}
		if count != 2 {
			dbt.Fatalf("Expected 2 matched rows, got %d", count)
		}
		res = dbt.mustExec("UPDATE test SET data = 1 WHERE id = 1")
		count, err = res.RowsAffected()
		if err != nil {
			dbt.Fatalf("res.RowsAffected() returned error: %s", err.Error())
		}
		if count != 3 {
			dbt.Fatalf("Expected 3 matched rows, got %d", count)
		}
	})
}

func TestTLS(t *testing.T) {
	tlsTestReq := func(dbt *DBTest) {
		if err := dbt.db.Ping(); err != nil {
			if err == ErrNoTLS {
				dbt.Skip("server does not support TLS")
			} else {
				dbt.Fatalf("error on Ping: %s", err.Error())
			}
		}

		rows := dbt.mustQuery("SHOW STATUS LIKE 'Ssl_cipher'")
		defer rows.Close()

		var variable, value *sql.RawBytes
		for rows.Next() {
			if err := rows.Scan(&variable, &value); err != nil {
				dbt.Fatal(err.Error())
			}

			if (*value == nil) || (len(*value) == 0) {
				dbt.Fatalf("no Cipher")
			} else {
				dbt.Logf("Cipher: %s", *value)
			}
		}
	}
	tlsTestOpt := func(dbt *DBTest) {
		if err := dbt.db.Ping(); err != nil {
			dbt.Fatalf("error on Ping: %s", err.Error())
		}
	}

	runTests(t, dsn+"&tls=preferred", tlsTestOpt)
	runTests(t, dsn+"&tls=skip-verify", tlsTestReq)

	// Verify that registering / using a custom cfg works
	RegisterTLSConfig("custom-skip-verify", &tls.Config{
		InsecureSkipVerify: true,
	})
	runTests(t, dsn+"&tls=custom-skip-verify", tlsTestReq)
}

func TestReuseClosedConnection(t *testing.T) {
	// this test does not use sql.database, it uses the driver directly
	if !available {
		t.Skipf("MySQL server not running on %s", netAddr)
	}

	md := &MySQLDriver{}
	conn, err := md.Open(dsn)
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}
	stmt, err := conn.Prepare("DO 1")
	if err != nil {
		t.Fatalf("error preparing statement: %s", err.Error())
	}
	_, err = stmt.Exec(nil)
	if err != nil {
		t.Fatalf("error executing statement: %s", err.Error())
	}
	err = conn.Close()
	if err != nil {
		t.Fatalf("error closing connection: %s", err.Error())
	}

	defer func() {
		if err := recover(); err != nil {
			t.Errorf("panic after reusing a closed connection: %v", err)
		}
	}()
	_, err = stmt.Exec(nil)
	if err != nil && err != driver.ErrBadConn {
		t.Errorf("unexpected error '%s', expected '%s'",
			err.Error(), driver.ErrBadConn.Error())
	}
}

func TestCharset(t *testing.T) {
	if !available {
		t.Skipf("MySQL server not running on %s", netAddr)
	}

	mustSetCharset := func(charsetParam, expected string) {
		runTests(t, dsn+"&"+charsetParam, func(dbt *DBTest) {
			rows := dbt.mustQuery("SELECT @@character_set_connection")
			defer rows.Close()

			if !rows.Next() {
				dbt.Fatalf("error getting connection charset: %s", rows.Err())
			}

			var got string
			rows.Scan(&got)

			if got != expected {
				dbt.Fatalf("expected connection charset %s but got %s", expected, got)
			}
		})
	}

	// non utf8 test
	mustSetCharset("charset=ascii", "ascii")

	// when the first charset is invalid, use the second
	mustSetCharset("charset=none,utf8mb4", "utf8mb4")

	// when the first charset is valid, use it
	mustSetCharset("charset=ascii,utf8mb4", "ascii")
	mustSetCharset("charset=utf8mb4,ascii", "utf8mb4")
}

func TestFailingCharset(t *testing.T) {
	runTests(t, dsn+"&charset=none", func(dbt *DBTest) {
		// run query to really establish connection...
		_, err := dbt.db.Exec("SELECT 1")
		if err == nil {
			dbt.db.Close()
			t.Fatalf("connection must not succeed without a valid charset")
		}
	})
}

func TestCollation(t *testing.T) {
	if !available {
		t.Skipf("MySQL server not running on %s", netAddr)
	}

	defaultCollation := "utf8mb4_general_ci"
	testCollations := []string{
		"",               // do not set
		defaultCollation, // driver default
		"latin1_general_ci",
		"binary",
		"utf8mb4_unicode_ci",
		"cp1257_bin",
	}

	for _, collation := range testCollations {
		var expected, tdsn string
		if collation != "" {
			tdsn = dsn + "&collation=" + collation
			expected = collation
		} else {
			tdsn = dsn
			expected = defaultCollation
		}

		runTests(t, tdsn, func(dbt *DBTest) {
			var got string
			if err := dbt.db.QueryRow("SELECT @@collation_connection").Scan(&got); err != nil {
				dbt.Fatal(err)
			}

			if got != expected {
				dbt.Fatalf("expected connection collation %s but got %s", expected, got)
			}
		})
	}
}

func TestColumnsWithAlias(t *testing.T) {
	runTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {
		rows := dbt.mustQuery("SELECT 1 AS A")
		defer rows.Close()
		cols, _ := rows.Columns()
		if len(cols) != 1 {
			t.Fatalf("expected 1 column, got %d", len(cols))
		}
		if cols[0] != "A" {
			t.Fatalf("expected column name \"A\", got \"%s\"", cols[0])
		}

		rows = dbt.mustQuery("SELECT * FROM (SELECT 1 AS one) AS A")
		defer rows.Close()
		cols, _ = rows.Columns()
		if len(cols) != 1 {
			t.Fatalf("expected 1 column, got %d", len(cols))
		}
		if cols[0] != "A.one" {
			t.Fatalf("expected column name \"A.one\", got \"%s\"", cols[0])
		}
	})
}

func TestRawBytesResultExceedsBuffer(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		// defaultBufSize from buffer.go
		expected := strings.Repeat("abc", defaultBufSize)

		rows := dbt.mustQuery("SELECT '" + expected + "'")
		defer rows.Close()
		if !rows.Next() {
			dbt.Error("expected result, got none")
		}
		var result sql.RawBytes
		rows.Scan(&result)
		if expected != string(result) {
			dbt.Error("result did not match expected value")
		}
	})
}

func TestTimezoneConversion(t *testing.T) {
	zones := []string{"UTC", "US/Central", "US/Pacific", "Local"}

	// Regression test for timezone handling
	tzTest := func(dbt *DBTest) {
		// Create table
		dbt.mustExec("CREATE TABLE test (ts TIMESTAMP)")

		// Insert local time into database (should be converted)
		usCentral, _ := time.LoadLocation("US/Central")
		reftime := time.Date(2014, 05, 30, 18, 03, 17, 0, time.UTC).In(usCentral)
		dbt.mustExec("INSERT INTO test VALUE (?)", reftime)

		// Retrieve time from DB
		rows := dbt.mustQuery("SELECT ts FROM test")
		defer rows.Close()
		if !rows.Next() {
			dbt.Fatal("did not get any rows out")
		}

		var dbTime time.Time
		err := rows.Scan(&dbTime)
		if err != nil {
			dbt.Fatal("Err", err)
		}

		// Check that dates match
		if reftime.Unix() != dbTime.Unix() {
			dbt.Errorf("times do not match.\n")
			dbt.Errorf(" Now(%v)=%v\n", usCentral, reftime)
			dbt.Errorf(" Now(UTC)=%v\n", dbTime)
		}
	}

	for _, tz := range zones {
		runTests(t, dsn+"&parseTime=true&loc="+url.QueryEscape(tz), tzTest)
	}
}

// Special cases

func TestRowsClose(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		rows, err := dbt.db.Query("SELECT 1")
		if err != nil {
			dbt.Fatal(err)
		}

		err = rows.Close()
		if err != nil {
			dbt.Fatal(err)
		}

		if rows.Next() {
			dbt.Fatal("unexpected row after rows.Close()")
		}

		err = rows.Err()
		if err != nil {
			dbt.Fatal(err)
		}
	})
}

// dangling statements
// http://code.google.com/p/go/issues/detail?id=3865
func TestCloseStmtBeforeRows(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		stmt, err := dbt.db.Prepare("SELECT 1")
		if err != nil {
			dbt.Fatal(err)
		}

		rows, err := stmt.Query()
		if err != nil {
			stmt.Close()
			dbt.Fatal(err)
		}
		defer rows.Close()

		err = stmt.Close()
		if err != nil {
			dbt.Fatal(err)
		}

		if !rows.Next() {
			dbt.Fatal("getting row failed")
		} else {
			err = rows.Err()
			if err != nil {
				dbt.Fatal(err)
			}

			var out bool
			err = rows.Scan(&out)
			if err != nil {
				dbt.Fatalf("error on rows.Scan(): %s", err.Error())
			}
			if out != true {
				dbt.Errorf("true != %t", out)
			}
		}
	})
}

// It is valid to have multiple Rows for the same Stmt
// http://code.google.com/p/go/issues/detail?id=3734
func TestStmtMultiRows(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		stmt, err := dbt.db.Prepare("SELECT 1 UNION SELECT 0")
		if err != nil {
			dbt.Fatal(err)
		}

		rows1, err := stmt.Query()
		if err != nil {
			stmt.Close()
			dbt.Fatal(err)
		}
		defer rows1.Close()

		rows2, err := stmt.Query()
		if err != nil {
			stmt.Close()
			dbt.Fatal(err)
		}
		defer rows2.Close()

		var out bool

		// 1
		if !rows1.Next() {
			dbt.Fatal("first rows1.Next failed")
		} else {
			err = rows1.Err()
			if err != nil {
				dbt.Fatal(err)
			}

			err = rows1.Scan(&out)
			if err != nil {
				dbt.Fatalf("error on rows.Scan(): %s", err.Error())
			}
			if out != true {
				dbt.Errorf("true != %t", out)
			}
		}

		if !rows2.Next() {
			dbt.Fatal("first rows2.Next failed")
		} else {
			err = rows2.Err()
			if err != nil {
				dbt.Fatal(err)
			}

			err = rows2.Scan(&out)
			if err != nil {
				dbt.Fatalf("error on rows.Scan(): %s", err.Error())
			}
			if out != true {
				dbt.Errorf("true != %t", out)
			}
		}

		// 2
		if !rows1.Next() {
			dbt.Fatal("second rows1.Next failed")
		} else {
			err = rows1.Err()
			if err != nil {
				dbt.Fatal(err)
			}

			err = rows1.Scan(&out)
			if err != nil {
				dbt.Fatalf("error on rows.Scan(): %s", err.Error())
			}
			if out != false {
				dbt.Errorf("false != %t", out)
			}

			if rows1.Next() {
				dbt.Fatal("unexpected row on rows1")
			}
			err = rows1.Close()
			if err != nil {
				dbt.Fatal(err)
			}
		}

		if !rows2.Next() {
			dbt.Fatal("second rows2.Next failed")
		} else {
			err = rows2.Err()
			if err != nil {
				dbt.Fatal(err)
			}

			err = rows2.Scan(&out)
			if err != nil {
				dbt.Fatalf("error on rows.Scan(): %s", err.Error())
			}
			if out != false {
				dbt.Errorf("false != %t", out)
			}

			if rows2.Next() {
				dbt.Fatal("unexpected row on rows2")
			}
			err = rows2.Close()
			if err != nil {
				dbt.Fatal(err)
			}
		}
	})
}

// Regression test for
// * more than 32 NULL parameters (issue 209)
// * more parameters than fit into the buffer (issue 201)
// * parameters * 64 > max_allowed_packet (issue 734)
func TestPreparedManyCols(t *testing.T) {
	numParams := 65535
	runTests(t, dsn, func(dbt *DBTest) {
		query := "SELECT ?" + strings.Repeat(",?", numParams-1)
		stmt, err := dbt.db.Prepare(query)
		if err != nil {
			dbt.Fatal(err)
		}
		defer stmt.Close()

		// create more parameters than fit into the buffer
		// which will take nil-values
		params := make([]interface{}, numParams)
		rows, err := stmt.Query(params...)
		if err != nil {
			dbt.Fatal(err)
		}
		rows.Close()

		// Create 0byte string which we can't send via STMT_LONG_DATA.
		for i := 0; i < numParams; i++ {
			params[i] = ""
		}
		rows, err = stmt.Query(params...)
		if err != nil {
			dbt.Fatal(err)
		}
		rows.Close()
	})
}

func TestConcurrent(t *testing.T) {
	if enabled, _ := readBool(os.Getenv("MYSQL_TEST_CONCURRENT")); !enabled {
		t.Skip("MYSQL_TEST_CONCURRENT env var not set")
	}

	runTests(t, dsn, func(dbt *DBTest) {
		var version string
		if err := dbt.db.QueryRow("SELECT @@version").Scan(&version); err != nil {
			dbt.Fatalf("%s", err.Error())
		}
		if strings.Contains(strings.ToLower(version), "mariadb") {
			t.Skip(`TODO: "fix commands out of sync. Did you run multiple statements at once?" on MariaDB`)
		}

		var max int
		err := dbt.db.QueryRow("SELECT @@max_connections").Scan(&max)
		if err != nil {
			dbt.Fatalf("%s", err.Error())
		}
		dbt.Logf("testing up to %d concurrent connections \r\n", max)

		var remaining, succeeded int32 = int32(max), 0

		var wg sync.WaitGroup
		wg.Add(max)

		var fatalError string
		var once sync.Once
		fatalf := func(s string, vals ...interface{}) {
			once.Do(func() {
				fatalError = fmt.Sprintf(s, vals...)
			})
		}

		for i := 0; i < max; i++ {
			go func(id int) {
				defer wg.Done()

				tx, err := dbt.db.Begin()
				atomic.AddInt32(&remaining, -1)

				if err != nil {
					if err.Error() != "Error 1040: Too many connections" {
						fatalf("error on conn %d: %s", id, err.Error())
					}
					return
				}

				// keep the connection busy until all connections are open
				for remaining > 0 {
					if _, err = tx.Exec("DO 1"); err != nil {
						fatalf("error on conn %d: %s", id, err.Error())
						return
					}
				}

				if err = tx.Commit(); err != nil {
					fatalf("error on conn %d: %s", id, err.Error())
					return
				}

				// everything went fine with this connection
				atomic.AddInt32(&succeeded, 1)
			}(i)
		}

		// wait until all conections are open
		wg.Wait()

		if fatalError != "" {
			dbt.Fatal(fatalError)
		}

		dbt.Logf("reached %d concurrent connections\r\n", succeeded)
	})
}

func testDialError(t *testing.T, dialErr error, expectErr error) {
	RegisterDialContext("mydial", func(ctx context.Context, addr string) (net.Conn, error) {
		return nil, dialErr
	})

	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@mydial(%s)/%s?timeout=30s", user, pass, addr, dbname))
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}
	defer db.Close()

	_, err = db.Exec("DO 1")
	if err != expectErr {
		t.Fatalf("was expecting %s. Got: %s", dialErr, err)
	}
}

func TestDialUnknownError(t *testing.T) {
	testErr := fmt.Errorf("test")
	testDialError(t, testErr, testErr)
}

func TestDialNonRetryableNetErr(t *testing.T) {
	testErr := netErrorMock{}
	testDialError(t, testErr, testErr)
}

func TestDialTemporaryNetErr(t *testing.T) {
	testErr := netErrorMock{temporary: true}
	testDialError(t, testErr, testErr)
}

// Tests custom dial functions
func TestCustomDial(t *testing.T) {
	if !available {
		t.Skipf("MySQL server not running on %s", netAddr)
	}

	// our custom dial function which justs wraps net.Dial here
	RegisterDialContext("mydial", func(ctx context.Context, addr string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, prot, addr)
	})

	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@mydial(%s)/%s?timeout=30s", user, pass, addr, dbname))
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}
	defer db.Close()

	if _, err = db.Exec("DO 1"); err != nil {
		t.Fatalf("connection failed: %s", err.Error())
	}
}

func TestSQLInjection(t *testing.T) {
	createTest := func(arg string) func(dbt *DBTest) {
		return func(dbt *DBTest) {
			dbt.mustExec("CREATE TABLE test (v INTEGER)")
			dbt.mustExec("INSERT INTO test VALUES (?)", 1)

			var v int
			// NULL can't be equal to anything, the idea here is to inject query so it returns row
			// This test verifies that escapeQuotes and escapeBackslash are working properly
			err := dbt.db.QueryRow("SELECT v FROM test WHERE NULL = ?", arg).Scan(&v)
			if err == sql.ErrNoRows {
				return // success, sql injection failed
			} else if err == nil {
				dbt.Errorf("sql injection successful with arg: %s", arg)
			} else {
				dbt.Errorf("error running query with arg: %s; err: %s", arg, err.Error())
			}
		}
	}

	dsns := []string{
		dsn,
		dsn + "&sql_mode='NO_BACKSLASH_ESCAPES'",
	}
	for _, testdsn := range dsns {
		runTests(t, testdsn, createTest("1 OR 1=1"))
		runTests(t, testdsn, createTest("' OR '1'='1"))
	}
}

// Test if inserted data is correctly retrieved after being escaped
func TestInsertRetrieveEscapedData(t *testing.T) {
	testData := func(dbt *DBTest) {
		dbt.mustExec("CREATE TABLE test (v VARCHAR(255))")

		// All sequences that are escaped by escapeQuotes and escapeBackslash
		v := "foo \x00\n\r\x1a\"'\\"
		dbt.mustExec("INSERT INTO test VALUES (?)", v)

		var out string
		err := dbt.db.QueryRow("SELECT v FROM test").Scan(&out)
		if err != nil {
			dbt.Fatalf("%s", err.Error())
		}

		if out != v {
			dbt.Errorf("%q != %q", out, v)
		}
	}

	dsns := []string{
		dsn,
		dsn + "&sql_mode='NO_BACKSLASH_ESCAPES'",
	}
	for _, testdsn := range dsns {
		runTests(t, testdsn, testData)
	}
}

func TestUnixSocketAuthFail(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		// Save the current logger so we can restore it.
		oldLogger := errLog

		// Set a new logger so we can capture its output.
		buffer := bytes.NewBuffer(make([]byte, 0, 64))
		newLogger := log.New(buffer, "prefix: ", 0)
		SetLogger(newLogger)

		// Restore the logger.
		defer SetLogger(oldLogger)

		// Make a new DSN that uses the MySQL socket file and a bad password, which
		// we can make by simply appending any character to the real password.
		badPass := pass + "x"
		socket := ""
		if prot == "unix" {
			socket = addr
		} else {
			// Get socket file from MySQL.
			err := dbt.db.QueryRow("SELECT @@socket").Scan(&socket)
			if err != nil {
				t.Fatalf("error on SELECT @@socket: %s", err.Error())
			}
		}
		t.Logf("socket: %s", socket)
		badDSN := fmt.Sprintf("%s:%s@unix(%s)/%s?timeout=30s", user, badPass, socket, dbname)
		db, err := sql.Open("mysql", badDSN)
		if err != nil {
			t.Fatalf("error connecting: %s", err.Error())
		}
		defer db.Close()

		// Connect to MySQL for real. This will cause an auth failure.
		err = db.Ping()
		if err == nil {
			t.Error("expected Ping() to return an error")
		}

		// The driver should not log anything.
		if actual := buffer.String(); actual != "" {
			t.Errorf("expected no output, got %q", actual)
		}
	})
}

// See Issue #422
func TestInterruptBySignal(t *testing.T) {
	runTestsWithMultiStatement(t, dsn, func(dbt *DBTest) {
		dbt.mustExec(`
			DROP PROCEDURE IF EXISTS test_signal;
			CREATE PROCEDURE test_signal(ret INT)
			BEGIN
				SELECT ret;
				SIGNAL SQLSTATE
					'45001'
				SET
					MESSAGE_TEXT = "an error",
					MYSQL_ERRNO = 45001;
			END
		`)
		defer dbt.mustExec("DROP PROCEDURE test_signal")

		var val int

		// text protocol
		rows, err := dbt.db.Query("CALL test_signal(42)")
		if err != nil {
			dbt.Fatalf("error on text query: %s", err.Error())
		}
		for rows.Next() {
			if err := rows.Scan(&val); err != nil {
				dbt.Error(err)
			} else if val != 42 {
				dbt.Errorf("expected val to be 42")
			}
		}
		rows.Close()

		// binary protocol
		rows, err = dbt.db.Query("CALL test_signal(?)", 42)
		if err != nil {
			dbt.Fatalf("error on binary query: %s", err.Error())
		}
		for rows.Next() {
			if err := rows.Scan(&val); err != nil {
				dbt.Error(err)
			} else if val != 42 {
				dbt.Errorf("expected val to be 42")
			}
		}
		rows.Close()
	})
}

func TestColumnsReusesSlice(t *testing.T) {
	rows := mysqlRows{
		rs: resultSet{
			columns: []mysqlField{
				{
					tableName: "test",
					name:      "A",
				},
				{
					tableName: "test",
					name:      "B",
				},
			},
		},
	}

	allocs := testing.AllocsPerRun(1, func() {
		cols := rows.Columns()

		if len(cols) != 2 {
			t.Fatalf("expected 2 columns, got %d", len(cols))
		}
	})

	if allocs != 0 {
		t.Fatalf("expected 0 allocations, got %d", int(allocs))
	}

	if rows.rs.columnNames == nil {
		t.Fatalf("expected columnNames to be set, got nil")
	}
}

func TestRejectReadOnly(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		// Create Table
		dbt.mustExec("CREATE TABLE test (value BOOL)")
		// Set the session to read-only. We didn't set the `rejectReadOnly`
		// option, so any writes after this should fail.
		_, err := dbt.db.Exec("SET SESSION TRANSACTION READ ONLY")
		// Error 1193: Unknown system variable 'TRANSACTION' => skip test,
		// MySQL server version is too old
		maybeSkip(t, err, 1193)
		if _, err := dbt.db.Exec("DROP TABLE test"); err == nil {
			t.Fatalf("writing to DB in read-only session without " +
				"rejectReadOnly did not error")
		}
		// Set the session back to read-write so runTests() can properly clean
		// up the table `test`.
		dbt.mustExec("SET SESSION TRANSACTION READ WRITE")
	})

	// Enable the `rejectReadOnly` option.
	runTests(t, dsn+"&rejectReadOnly=true", func(dbt *DBTest) {
		// Create Table
		dbt.mustExec("CREATE TABLE test (value BOOL)")
		// Set the session to read only. Any writes after this should error on
		// a driver.ErrBadConn, and cause `database/sql` to initiate a new
		// connection.
		dbt.mustExec("SET SESSION TRANSACTION READ ONLY")
		// This would error, but `database/sql` should automatically retry on a
		// new connection which is not read-only, and eventually succeed.
		dbt.mustExec("DROP TABLE test")
	})
}

func TestPing(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		if err := dbt.db.Ping(); err != nil {
			dbt.fail("Ping", "Ping", err)
		}
	})
}

// See Issue #799
func TestEmptyPassword(t *testing.T) {
	if !available {
		t.Skipf("MySQL server not running on %s", netAddr)
	}

	dsn := fmt.Sprintf("%s:%s@%s/%s?timeout=30s", user, "", netAddr, dbname)
	db, err := sql.Open("mysql", dsn)
	if err == nil {
		defer db.Close()
		err = db.Ping()
	}

	if pass == "" {
		if err != nil {
			t.Fatal(err.Error())
		}
	} else {
		if err == nil {
			t.Fatal("expected authentication error")
		}
		if !strings.HasPrefix(err.Error(), "Error 1045") {
			t.Fatal(err.Error())
		}
	}
}

// static interface implementation checks of mysqlConn
var (
	_ driver.ConnBeginTx        = &mysqlConn{}
	_ driver.ConnPrepareContext = &mysqlConn{}
	_ driver.ExecerContext      = &mysqlConn{}
	_ driver.Pinger             = &mysqlConn{}
	_ driver.QueryerContext     = &mysqlConn{}
)

// static interface implementation checks of mysqlStmt
var (
	_ driver.StmtExecContext  = &mysqlStmt{}
	_ driver.StmtQueryContext = &mysqlStmt{}
)

// Ensure that all the driver interfaces are implemented
var (
	// _ driver.RowsColumnTypeLength        = &binaryRows{}
	// _ driver.RowsColumnTypeLength        = &textRows{}
	_ driver.RowsColumnTypeDatabaseTypeName = &binaryRows{}
	_ driver.RowsColumnTypeDatabaseTypeName = &textRows{}
	_ driver.RowsColumnTypeNullable         = &binaryRows{}
	_ driver.RowsColumnTypeNullable         = &textRows{}
	_ driver.RowsColumnTypePrecisionScale   = &binaryRows{}
	_ driver.RowsColumnTypePrecisionScale   = &textRows{}
	_ driver.RowsColumnTypeScanType         = &binaryRows{}
	_ driver.RowsColumnTypeScanType         = &textRows{}
	_ driver.RowsNextResultSet              = &binaryRows{}
	_ driver.RowsNextResultSet              = &textRows{}
)

func TestMultiResultSet(t *testing.T) {
	type result struct {
		values  [][]int
		columns []string
	}

	// checkRows is a helper test function to validate rows containing 3 result
	// sets with specific values and columns. The basic query would look like this:
	//
	// SELECT 1 AS col1, 2 AS col2 UNION SELECT 3, 4;
	// SELECT 0 UNION SELECT 1;
	// SELECT 1 AS col1, 2 AS col2, 3 AS col3 UNION SELECT 4, 5, 6;
	//
	// to distinguish test cases the first string argument is put in front of
	// every error or fatal message.
	checkRows := func(desc string, rows *sql.Rows, dbt *DBTest) {
		expected := []result{
			{
				values:  [][]int{{1, 2}, {3, 4}},
				columns: []string{"col1", "col2"},
			},
			{
				values:  [][]int{{1, 2, 3}, {4, 5, 6}},
				columns: []string{"col1", "col2", "col3"},
			},
		}

		var res1 result
		for rows.Next() {
			var res [2]int
			if err := rows.Scan(&res[0], &res[1]); err != nil {
				dbt.Fatal(err)
			}
			res1.values = append(res1.values, res[:])
		}

		cols, err := rows.Columns()
		if err != nil {
			dbt.Fatal(desc, err)
		}
		res1.columns = cols

		if !reflect.DeepEqual(expected[0], res1) {
			dbt.Error(desc, "want =", expected[0], "got =", res1)
		}

		if !rows.NextResultSet() {
			dbt.Fatal(desc, "expected next result set")
		}

		// ignoring one result set

		if !rows.NextResultSet() {
			dbt.Fatal(desc, "expected next result set")
		}

		var res2 result
		cols, err = rows.Columns()
		if err != nil {
			dbt.Fatal(desc, err)
		}
		res2.columns = cols

		for rows.Next() {
			var res [3]int
			if err := rows.Scan(&res[0], &res[1], &res[2]); err != nil {
				dbt.Fatal(desc, err)
			}
			res2.values = append(res2.values, res[:])
		}

		if !reflect.DeepEqual(expected[1], res2) {
			dbt.Error(desc, "want =", expected[1], "got =", res2)
		}

		if rows.NextResultSet() {
			dbt.Error(desc, "unexpected next result set")
		}

		if err := rows.Err(); err != nil {
			dbt.Error(desc, err)
		}
	}

	runTestsWithMultiStatement(t, dsn, func(dbt *DBTest) {
		rows := dbt.mustQuery(`DO 1;
		SELECT 1 AS col1, 2 AS col2 UNION SELECT 3, 4;
		DO 1;
		SELECT 0 UNION SELECT 1;
		SELECT 1 AS col1, 2 AS col2, 3 AS col3 UNION SELECT 4, 5, 6;`)
		defer rows.Close()
		checkRows("query: ", rows, dbt)
	})

	runTestsWithMultiStatement(t, dsn, func(dbt *DBTest) {
		queries := []string{
			`
			DROP PROCEDURE IF EXISTS test_mrss;
			CREATE PROCEDURE test_mrss()
			BEGIN
				DO 1;
				SELECT 1 AS col1, 2 AS col2 UNION SELECT 3, 4;
				DO 1;
				SELECT 0 UNION SELECT 1;
				SELECT 1 AS col1, 2 AS col2, 3 AS col3 UNION SELECT 4, 5, 6;
			END
		`,
			`
			DROP PROCEDURE IF EXISTS test_mrss;
			CREATE PROCEDURE test_mrss()
			BEGIN
				SELECT 1 AS col1, 2 AS col2 UNION SELECT 3, 4;
				SELECT 0 UNION SELECT 1;
				SELECT 1 AS col1, 2 AS col2, 3 AS col3 UNION SELECT 4, 5, 6;
			END
		`,
		}

		defer dbt.mustExec("DROP PROCEDURE IF EXISTS test_mrss")

		for i, query := range queries {
			dbt.mustExec(query)

			stmt, err := dbt.db.Prepare("CALL test_mrss()")
			if err != nil {
				dbt.Fatalf("%v (i=%d)", err, i)
			}
			defer stmt.Close()

			for j := 0; j < 2; j++ {
				rows, err := stmt.Query()
				if err != nil {
					dbt.Fatalf("%v (i=%d) (j=%d)", err, i, j)
				}
				checkRows(fmt.Sprintf("prepared stmt query (i=%d) (j=%d): ", i, j), rows, dbt)
			}
		}
	})
}

func TestMultiResultSetNoSelect(t *testing.T) {
	runTestsWithMultiStatement(t, dsn, func(dbt *DBTest) {
		rows := dbt.mustQuery("DO 1; DO 2;")
		defer rows.Close()

		if rows.Next() {
			dbt.Error("unexpected row")
		}

		if rows.NextResultSet() {
			dbt.Error("unexpected next result set")
		}

		if err := rows.Err(); err != nil {
			dbt.Error("expected nil; got ", err)
		}
	})
}

// tests if rows are set in a proper state if some results were ignored before
// calling rows.NextResultSet.
func TestSkipResults(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		rows := dbt.mustQuery("SELECT 1, 2")
		defer rows.Close()

		if !rows.Next() {
			dbt.Error("expected row")
		}

		if rows.NextResultSet() {
			dbt.Error("unexpected next result set")
		}

		if err := rows.Err(); err != nil {
			dbt.Error("expected nil; got ", err)
		}
	})
}

func TestPingContext(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := dbt.db.PingContext(ctx); err != context.Canceled {
			dbt.Errorf("expected context.Canceled, got %v", err)
		}
	})
}

func TestContextCancelExec(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("CREATE TABLE test (v INTEGER)")
		ctx, cancel := context.WithCancel(context.Background())

		// Delay execution for just a bit until db.ExecContext has begun.
		defer time.AfterFunc(250*time.Millisecond, cancel).Stop()

		// This query will be canceled.
		startTime := time.Now()
		if _, err := dbt.db.ExecContext(ctx, "INSERT INTO test VALUES (SLEEP(1))"); err != context.Canceled {
			dbt.Errorf("expected context.Canceled, got %v", err)
		}
		if d := time.Since(startTime); d > 500*time.Millisecond {
			dbt.Errorf("too long execution time: %s", d)
		}

		// Wait for the INSERT query to be done.
		time.Sleep(time.Second)

		// Check how many times the query is executed.
		var v int
		if err := dbt.db.QueryRow("SELECT COUNT(*) FROM test").Scan(&v); err != nil {
			dbt.Fatalf("%s", err.Error())
		}
		if v != 1 { // TODO: need to kill the query, and v should be 0.
			dbt.Skipf("[WARN] expected val to be 1, got %d", v)
		}

		// Context is already canceled, so error should come before execution.
		if _, err := dbt.db.ExecContext(ctx, "INSERT INTO test VALUES (1)"); err == nil {
			dbt.Error("expected error")
		} else if err.Error() != "context canceled" {
			dbt.Fatalf("unexpected error: %s", err)
		}

		// The second insert query will fail, so the table has no changes.
		if err := dbt.db.QueryRow("SELECT COUNT(*) FROM test").Scan(&v); err != nil {
			dbt.Fatalf("%s", err.Error())
		}
		if v != 1 {
			dbt.Skipf("[WARN] expected val to be 1, got %d", v)
		}
	})
}

func TestContextCancelQuery(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("CREATE TABLE test (v INTEGER)")
		ctx, cancel := context.WithCancel(context.Background())

		// Delay execution for just a bit until db.ExecContext has begun.
		defer time.AfterFunc(250*time.Millisecond, cancel).Stop()

		// This query will be canceled.
		startTime := time.Now()
		if _, err := dbt.db.QueryContext(ctx, "INSERT INTO test VALUES (SLEEP(1))"); err != context.Canceled {
			dbt.Errorf("expected context.Canceled, got %v", err)
		}
		if d := time.Since(startTime); d > 500*time.Millisecond {
			dbt.Errorf("too long execution time: %s", d)
		}

		// Wait for the INSERT query to be done.
		time.Sleep(time.Second)

		// Check how many times the query is executed.
		var v int
		if err := dbt.db.QueryRow("SELECT COUNT(*) FROM test").Scan(&v); err != nil {
			dbt.Fatalf("%s", err.Error())
		}
		if v != 1 { // TODO: need to kill the query, and v should be 0.
			dbt.Skipf("[WARN] expected val to be 1, got %d", v)
		}

		// Context is already canceled, so error should come before execution.
		if _, err := dbt.db.QueryContext(ctx, "INSERT INTO test VALUES (1)"); err != context.Canceled {
			dbt.Errorf("expected context.Canceled, got %v", err)
		}

		// The second insert query will fail, so the table has no changes.
		if err := dbt.db.QueryRow("SELECT COUNT(*) FROM test").Scan(&v); err != nil {
			dbt.Fatalf("%s", err.Error())
		}
		if v != 1 {
			dbt.Skipf("[WARN] expected val to be 1, got %d", v)
		}
	})
}

func TestContextCancelQueryRow(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("CREATE TABLE test (v INTEGER)")
		dbt.mustExec("INSERT INTO test VALUES (1), (2), (3)")
		ctx, cancel := context.WithCancel(context.Background())

		rows, err := dbt.db.QueryContext(ctx, "SELECT v FROM test")
		if err != nil {
			dbt.Fatalf("%s", err.Error())
		}

		// the first row will be succeed.
		var v int
		if !rows.Next() {
			dbt.Fatalf("unexpected end")
		}
		if err := rows.Scan(&v); err != nil {
			dbt.Fatalf("%s", err.Error())
		}

		cancel()
		// make sure the driver receives the cancel request.
		time.Sleep(100 * time.Millisecond)

		if rows.Next() {
			dbt.Errorf("expected end, but not")
		}
		if err := rows.Err(); err != context.Canceled {
			dbt.Errorf("expected context.Canceled, got %v", err)
		}
	})
}

func TestContextCancelPrepare(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := dbt.db.PrepareContext(ctx, "SELECT 1"); err != context.Canceled {
			dbt.Errorf("expected context.Canceled, got %v", err)
		}
	})
}

func TestContextCancelStmtExec(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("CREATE TABLE test (v INTEGER)")
		ctx, cancel := context.WithCancel(context.Background())
		stmt, err := dbt.db.PrepareContext(ctx, "INSERT INTO test VALUES (SLEEP(1))")
		if err != nil {
			dbt.Fatalf("unexpected error: %v", err)
		}

		// Delay execution for just a bit until db.ExecContext has begun.
		defer time.AfterFunc(250*time.Millisecond, cancel).Stop()

		// This query will be canceled.
		startTime := time.Now()
		if _, err := stmt.ExecContext(ctx); err != context.Canceled {
			dbt.Errorf("expected context.Canceled, got %v", err)
		}
		if d := time.Since(startTime); d > 500*time.Millisecond {
			dbt.Errorf("too long execution time: %s", d)
		}

		// Wait for the INSERT query to be done.
		time.Sleep(time.Second)

		// Check how many times the query is executed.
		var v int
		if err := dbt.db.QueryRow("SELECT COUNT(*) FROM test").Scan(&v); err != nil {
			dbt.Fatalf("%s", err.Error())
		}
		if v != 1 { // TODO: need to kill the query, and v should be 0.
			dbt.Skipf("[WARN] expected val to be 1, got %d", v)
		}
	})
}

func TestContextCancelStmtQuery(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("CREATE TABLE test (v INTEGER)")
		ctx, cancel := context.WithCancel(context.Background())
		stmt, err := dbt.db.PrepareContext(ctx, "INSERT INTO test VALUES (SLEEP(1))")
		if err != nil {
			dbt.Fatalf("unexpected error: %v", err)
		}

		// Delay execution for just a bit until db.ExecContext has begun.
		defer time.AfterFunc(250*time.Millisecond, cancel).Stop()

		// This query will be canceled.
		startTime := time.Now()
		if _, err := stmt.QueryContext(ctx); err != context.Canceled {
			dbt.Errorf("expected context.Canceled, got %v", err)
		}
		if d := time.Since(startTime); d > 500*time.Millisecond {
			dbt.Errorf("too long execution time: %s", d)
		}

		// Wait for the INSERT query has done.
		time.Sleep(time.Second)

		// Check how many times the query is executed.
		var v int
		if err := dbt.db.QueryRow("SELECT COUNT(*) FROM test").Scan(&v); err != nil {
			dbt.Fatalf("%s", err.Error())
		}
		if v != 1 { // TODO: need to kill the query, and v should be 0.
			dbt.Skipf("[WARN] expected val to be 1, got %d", v)
		}
	})
}

func TestContextCancelBegin(t *testing.T) {
	if runtime.GOOS == "windows" || runtime.GOOS == "darwin" {
		t.Skip(`FIXME: it sometime fails with "expected driver.ErrBadConn, got sql: connection is already closed" on windows and macOS`)
	}

	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("CREATE TABLE test (v INTEGER)")
		ctx, cancel := context.WithCancel(context.Background())
		conn, err := dbt.db.Conn(ctx)
		if err != nil {
			dbt.Fatal(err)
		}
		defer conn.Close()
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			dbt.Fatal(err)
		}

		// Delay execution for just a bit until db.ExecContext has begun.
		defer time.AfterFunc(100*time.Millisecond, cancel).Stop()

		// This query will be canceled.
		startTime := time.Now()
		if _, err := tx.ExecContext(ctx, "INSERT INTO test VALUES (SLEEP(1))"); err != context.Canceled {
			dbt.Errorf("expected context.Canceled, got %v", err)
		}
		if d := time.Since(startTime); d > 500*time.Millisecond {
			dbt.Errorf("too long execution time: %s", d)
		}

		// Transaction is canceled, so expect an error.
		switch err := tx.Commit(); err {
		case sql.ErrTxDone:
			// because the transaction has already been rollbacked.
			// the database/sql package watches ctx
			// and rollbacks when ctx is canceled.
		case context.Canceled:
			// the database/sql package rollbacks on another goroutine,
			// so the transaction may not be rollbacked depending on goroutine scheduling.
		default:
			dbt.Errorf("expected sql.ErrTxDone or context.Canceled, got %v", err)
		}

		// The connection is now in an inoperable state - so performing other
		// operations should fail with ErrBadConn
		// Important to exercise isolation level too - it runs SET TRANSACTION ISOLATION
		// LEVEL XXX first, which needs to return ErrBadConn if the connection's context
		// is cancelled
		_, err = conn.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelReadCommitted})
		if err != driver.ErrBadConn {
			dbt.Errorf("expected driver.ErrBadConn, got %v", err)
		}

		// cannot begin a transaction (on a different conn) with a canceled context
		if _, err := dbt.db.BeginTx(ctx, nil); err != context.Canceled {
			dbt.Errorf("expected context.Canceled, got %v", err)
		}
	})
}

func TestContextBeginIsolationLevel(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("CREATE TABLE test (v INTEGER)")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tx1, err := dbt.db.BeginTx(ctx, &sql.TxOptions{
			Isolation: sql.LevelRepeatableRead,
		})
		if err != nil {
			dbt.Fatal(err)
		}

		tx2, err := dbt.db.BeginTx(ctx, &sql.TxOptions{
			Isolation: sql.LevelReadCommitted,
		})
		if err != nil {
			dbt.Fatal(err)
		}

		_, err = tx1.ExecContext(ctx, "INSERT INTO test VALUES (1)")
		if err != nil {
			dbt.Fatal(err)
		}

		var v int
		row := tx2.QueryRowContext(ctx, "SELECT COUNT(*) FROM test")
		if err := row.Scan(&v); err != nil {
			dbt.Fatal(err)
		}
		// Because writer transaction wasn't commited yet, it should be available
		if v != 0 {
			dbt.Errorf("expected val to be 0, got %d", v)
		}

		err = tx1.Commit()
		if err != nil {
			dbt.Fatal(err)
		}

		row = tx2.QueryRowContext(ctx, "SELECT COUNT(*) FROM test")
		if err := row.Scan(&v); err != nil {
			dbt.Fatal(err)
		}
		// Data written by writer transaction is already commited, it should be selectable
		if v != 1 {
			dbt.Errorf("expected val to be 1, got %d", v)
		}
		tx2.Commit()
	})
}

func TestContextBeginReadOnly(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("CREATE TABLE test (v INTEGER)")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tx, err := dbt.db.BeginTx(ctx, &sql.TxOptions{
			ReadOnly: true,
		})
		if _, ok := err.(*MySQLError); ok {
			dbt.Skip("It seems that your MySQL does not support READ ONLY transactions")
			return
		} else if err != nil {
			dbt.Fatal(err)
		}

		// INSERT queries fail in a READ ONLY transaction.
		_, err = tx.ExecContext(ctx, "INSERT INTO test VALUES (1)")
		if _, ok := err.(*MySQLError); !ok {
			dbt.Errorf("expected MySQLError, got %v", err)
		}

		// SELECT queries can be executed.
		var v int
		row := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM test")
		if err := row.Scan(&v); err != nil {
			dbt.Fatal(err)
		}
		if v != 0 {
			dbt.Errorf("expected val to be 0, got %d", v)
		}

		if err := tx.Commit(); err != nil {
			dbt.Fatal(err)
		}
	})
}

func TestRowsColumnTypes(t *testing.T) {
	niNULL := sql.NullInt64{Int64: 0, Valid: false}
	ni0 := sql.NullInt64{Int64: 0, Valid: true}
	ni1 := sql.NullInt64{Int64: 1, Valid: true}
	ni42 := sql.NullInt64{Int64: 42, Valid: true}
	nfNULL := sql.NullFloat64{Float64: 0.0, Valid: false}
	nf0 := sql.NullFloat64{Float64: 0.0, Valid: true}
	nf1337 := sql.NullFloat64{Float64: 13.37, Valid: true}
	nt0 := sql.NullTime{Time: time.Date(2006, 01, 02, 15, 04, 05, 0, time.UTC), Valid: true}
	nt1 := sql.NullTime{Time: time.Date(2006, 01, 02, 15, 04, 05, 100000000, time.UTC), Valid: true}
	nt2 := sql.NullTime{Time: time.Date(2006, 01, 02, 15, 04, 05, 110000000, time.UTC), Valid: true}
	nt6 := sql.NullTime{Time: time.Date(2006, 01, 02, 15, 04, 05, 111111000, time.UTC), Valid: true}
	nd1 := sql.NullTime{Time: time.Date(2006, 01, 02, 0, 0, 0, 0, time.UTC), Valid: true}
	nd2 := sql.NullTime{Time: time.Date(2006, 03, 04, 0, 0, 0, 0, time.UTC), Valid: true}
	ndNULL := sql.NullTime{Time: time.Time{}, Valid: false}
	rbNULL := sql.RawBytes(nil)
	rb0 := sql.RawBytes("0")
	rb42 := sql.RawBytes("42")
	rbTest := sql.RawBytes("Test")
	rb0pad4 := sql.RawBytes("0\x00\x00\x00") // BINARY right-pads values with 0x00
	rbx0 := sql.RawBytes("\x00")
	rbx42 := sql.RawBytes("\x42")

	var columns = []struct {
		name             string
		fieldType        string // type used when creating table schema
		databaseTypeName string // actual type used by MySQL
		scanType         reflect.Type
		nullable         bool
		precision        int64 // 0 if not ok
		scale            int64
		valuesIn         [3]string
		valuesOut        [3]interface{}
	}{
		{"bit8null", "BIT(8)", "BIT", scanTypeRawBytes, true, 0, 0, [3]string{"0x0", "NULL", "0x42"}, [3]interface{}{rbx0, rbNULL, rbx42}},
		{"boolnull", "BOOL", "TINYINT", scanTypeNullInt, true, 0, 0, [3]string{"NULL", "true", "0"}, [3]interface{}{niNULL, ni1, ni0}},
		{"bool", "BOOL NOT NULL", "TINYINT", scanTypeInt8, false, 0, 0, [3]string{"1", "0", "FALSE"}, [3]interface{}{int8(1), int8(0), int8(0)}},
		{"intnull", "INTEGER", "INT", scanTypeNullInt, true, 0, 0, [3]string{"0", "NULL", "42"}, [3]interface{}{ni0, niNULL, ni42}},
		{"smallint", "SMALLINT NOT NULL", "SMALLINT", scanTypeInt16, false, 0, 0, [3]string{"0", "-32768", "32767"}, [3]interface{}{int16(0), int16(-32768), int16(32767)}},
		{"smallintnull", "SMALLINT", "SMALLINT", scanTypeNullInt, true, 0, 0, [3]string{"0", "NULL", "42"}, [3]interface{}{ni0, niNULL, ni42}},
		{"int3null", "INT(3)", "INT", scanTypeNullInt, true, 0, 0, [3]string{"0", "NULL", "42"}, [3]interface{}{ni0, niNULL, ni42}},
		{"int7", "INT(7) NOT NULL", "INT", scanTypeInt32, false, 0, 0, [3]string{"0", "-1337", "42"}, [3]interface{}{int32(0), int32(-1337), int32(42)}},
		{"mediumintnull", "MEDIUMINT", "MEDIUMINT", scanTypeNullInt, true, 0, 0, [3]string{"0", "42", "NULL"}, [3]interface{}{ni0, ni42, niNULL}},
		{"bigint", "BIGINT NOT NULL", "BIGINT", scanTypeInt64, false, 0, 0, [3]string{"0", "65535", "-42"}, [3]interface{}{int64(0), int64(65535), int64(-42)}},
		{"bigintnull", "BIGINT", "BIGINT", scanTypeNullInt, true, 0, 0, [3]string{"NULL", "1", "42"}, [3]interface{}{niNULL, ni1, ni42}},
		{"tinyuint", "TINYINT UNSIGNED NOT NULL", "UNSIGNED TINYINT", scanTypeUint8, false, 0, 0, [3]string{"0", "255", "42"}, [3]interface{}{uint8(0), uint8(255), uint8(42)}},
		{"smalluint", "SMALLINT UNSIGNED NOT NULL", "UNSIGNED SMALLINT", scanTypeUint16, false, 0, 0, [3]string{"0", "65535", "42"}, [3]interface{}{uint16(0), uint16(65535), uint16(42)}},
		{"biguint", "BIGINT UNSIGNED NOT NULL", "UNSIGNED BIGINT", scanTypeUint64, false, 0, 0, [3]string{"0", "65535", "42"}, [3]interface{}{uint64(0), uint64(65535), uint64(42)}},
		{"uint13", "INT(13) UNSIGNED NOT NULL", "UNSIGNED INT", scanTypeUint32, false, 0, 0, [3]string{"0", "1337", "42"}, [3]interface{}{uint32(0), uint32(1337), uint32(42)}},
		{"float", "FLOAT NOT NULL", "FLOAT", scanTypeFloat32, false, math.MaxInt64, math.MaxInt64, [3]string{"0", "42", "13.37"}, [3]interface{}{float32(0), float32(42), float32(13.37)}},
		{"floatnull", "FLOAT", "FLOAT", scanTypeNullFloat, true, math.MaxInt64, math.MaxInt64, [3]string{"0", "NULL", "13.37"}, [3]interface{}{nf0, nfNULL, nf1337}},
		{"float74null", "FLOAT(7,4)", "FLOAT", scanTypeNullFloat, true, math.MaxInt64, 4, [3]string{"0", "NULL", "13.37"}, [3]interface{}{nf0, nfNULL, nf1337}},
		{"double", "DOUBLE NOT NULL", "DOUBLE", scanTypeFloat64, false, math.MaxInt64, math.MaxInt64, [3]string{"0", "42", "13.37"}, [3]interface{}{float64(0), float64(42), float64(13.37)}},
		{"doublenull", "DOUBLE", "DOUBLE", scanTypeNullFloat, true, math.MaxInt64, math.MaxInt64, [3]string{"0", "NULL", "13.37"}, [3]interface{}{nf0, nfNULL, nf1337}},
		{"decimal1", "DECIMAL(10,6) NOT NULL", "DECIMAL", scanTypeRawBytes, false, 10, 6, [3]string{"0", "13.37", "1234.123456"}, [3]interface{}{sql.RawBytes("0.000000"), sql.RawBytes("13.370000"), sql.RawBytes("1234.123456")}},
		{"decimal1null", "DECIMAL(10,6)", "DECIMAL", scanTypeRawBytes, true, 10, 6, [3]string{"0", "NULL", "1234.123456"}, [3]interface{}{sql.RawBytes("0.000000"), rbNULL, sql.RawBytes("1234.123456")}},
		{"decimal2", "DECIMAL(8,4) NOT NULL", "DECIMAL", scanTypeRawBytes, false, 8, 4, [3]string{"0", "13.37", "1234.123456"}, [3]interface{}{sql.RawBytes("0.0000"), sql.RawBytes("13.3700"), sql.RawBytes("1234.1235")}},
		{"decimal2null", "DECIMAL(8,4)", "DECIMAL", scanTypeRawBytes, true, 8, 4, [3]string{"0", "NULL", "1234.123456"}, [3]interface{}{sql.RawBytes("0.0000"), rbNULL, sql.RawBytes("1234.1235")}},
		{"decimal3", "DECIMAL(5,0) NOT NULL", "DECIMAL", scanTypeRawBytes, false, 5, 0, [3]string{"0", "13.37", "-12345.123456"}, [3]interface{}{rb0, sql.RawBytes("13"), sql.RawBytes("-12345")}},
		{"decimal3null", "DECIMAL(5,0)", "DECIMAL", scanTypeRawBytes, true, 5, 0, [3]string{"0", "NULL", "-12345.123456"}, [3]interface{}{rb0, rbNULL, sql.RawBytes("-12345")}},
		{"char25null", "CHAR(25)", "CHAR", scanTypeRawBytes, true, 0, 0, [3]string{"0", "NULL", "'Test'"}, [3]interface{}{rb0, rbNULL, rbTest}},
		{"varchar42", "VARCHAR(42) NOT NULL", "VARCHAR", scanTypeRawBytes, false, 0, 0, [3]string{"0", "'Test'", "42"}, [3]interface{}{rb0, rbTest, rb42}},
		{"binary4null", "BINARY(4)", "BINARY", scanTypeRawBytes, true, 0, 0, [3]string{"0", "NULL", "'Test'"}, [3]interface{}{rb0pad4, rbNULL, rbTest}},
		{"varbinary42", "VARBINARY(42) NOT NULL", "VARBINARY", scanTypeRawBytes, false, 0, 0, [3]string{"0", "'Test'", "42"}, [3]interface{}{rb0, rbTest, rb42}},
		{"tinyblobnull", "TINYBLOB", "BLOB", scanTypeRawBytes, true, 0, 0, [3]string{"0", "NULL", "'Test'"}, [3]interface{}{rb0, rbNULL, rbTest}},
		{"tinytextnull", "TINYTEXT", "TEXT", scanTypeRawBytes, true, 0, 0, [3]string{"0", "NULL", "'Test'"}, [3]interface{}{rb0, rbNULL, rbTest}},
		{"blobnull", "BLOB", "BLOB", scanTypeRawBytes, true, 0, 0, [3]string{"0", "NULL", "'Test'"}, [3]interface{}{rb0, rbNULL, rbTest}},
		{"textnull", "TEXT", "TEXT", scanTypeRawBytes, true, 0, 0, [3]string{"0", "NULL", "'Test'"}, [3]interface{}{rb0, rbNULL, rbTest}},
		{"mediumblob", "MEDIUMBLOB NOT NULL", "BLOB", scanTypeRawBytes, false, 0, 0, [3]string{"0", "'Test'", "42"}, [3]interface{}{rb0, rbTest, rb42}},
		{"mediumtext", "MEDIUMTEXT NOT NULL", "TEXT", scanTypeRawBytes, false, 0, 0, [3]string{"0", "'Test'", "42"}, [3]interface{}{rb0, rbTest, rb42}},
		{"longblob", "LONGBLOB NOT NULL", "BLOB", scanTypeRawBytes, false, 0, 0, [3]string{"0", "'Test'", "42"}, [3]interface{}{rb0, rbTest, rb42}},
		{"longtext", "LONGTEXT NOT NULL", "TEXT", scanTypeRawBytes, false, 0, 0, [3]string{"0", "'Test'", "42"}, [3]interface{}{rb0, rbTest, rb42}},
		{"datetime", "DATETIME", "DATETIME", scanTypeNullTime, true, 0, 0, [3]string{"'2006-01-02 15:04:05'", "'2006-01-02 15:04:05.1'", "'2006-01-02 15:04:05.111111'"}, [3]interface{}{nt0, nt0, nt0}},
		{"datetime2", "DATETIME(2)", "DATETIME", scanTypeNullTime, true, 2, 2, [3]string{"'2006-01-02 15:04:05'", "'2006-01-02 15:04:05.1'", "'2006-01-02 15:04:05.111111'"}, [3]interface{}{nt0, nt1, nt2}},
		{"datetime6", "DATETIME(6)", "DATETIME", scanTypeNullTime, true, 6, 6, [3]string{"'2006-01-02 15:04:05'", "'2006-01-02 15:04:05.1'", "'2006-01-02 15:04:05.111111'"}, [3]interface{}{nt0, nt1, nt6}},
		{"date", "DATE", "DATE", scanTypeNullTime, true, 0, 0, [3]string{"'2006-01-02'", "NULL", "'2006-03-04'"}, [3]interface{}{nd1, ndNULL, nd2}},
		{"year", "YEAR NOT NULL", "YEAR", scanTypeUint16, false, 0, 0, [3]string{"2006", "2000", "1994"}, [3]interface{}{uint16(2006), uint16(2000), uint16(1994)}},
	}

	schema := ""
	values1 := ""
	values2 := ""
	values3 := ""
	for _, column := range columns {
		schema += fmt.Sprintf("`%s` %s, ", column.name, column.fieldType)
		values1 += column.valuesIn[0] + ", "
		values2 += column.valuesIn[1] + ", "
		values3 += column.valuesIn[2] + ", "
	}
	schema = schema[:len(schema)-2]
	values1 = values1[:len(values1)-2]
	values2 = values2[:len(values2)-2]
	values3 = values3[:len(values3)-2]

	runTests(t, dsn+"&parseTime=true", func(dbt *DBTest) {
		dbt.mustExec("CREATE TABLE test (" + schema + ")")
		dbt.mustExec("INSERT INTO test VALUES (" + values1 + "), (" + values2 + "), (" + values3 + ")")

		rows, err := dbt.db.Query("SELECT * FROM test")
		if err != nil {
			t.Fatalf("Query: %v", err)
		}

		tt, err := rows.ColumnTypes()
		if err != nil {
			t.Fatalf("ColumnTypes: %v", err)
		}

		if len(tt) != len(columns) {
			t.Fatalf("unexpected number of columns: expected %d, got %d", len(columns), len(tt))
		}

		types := make([]reflect.Type, len(tt))
		for i, tp := range tt {
			column := columns[i]

			// Name
			name := tp.Name()
			if name != column.name {
				t.Errorf("column name mismatch %s != %s", name, column.name)
				continue
			}

			// DatabaseTypeName
			databaseTypeName := tp.DatabaseTypeName()
			if databaseTypeName != column.databaseTypeName {
				t.Errorf("databasetypename name mismatch for column %q: %s != %s", name, databaseTypeName, column.databaseTypeName)
				continue
			}

			// ScanType
			scanType := tp.ScanType()
			if scanType != column.scanType {
				if scanType == nil {
					t.Errorf("scantype is null for column %q", name)
				} else {
					t.Errorf("scantype mismatch for column %q: %s != %s", name, scanType.Name(), column.scanType.Name())
				}
				continue
			}
			types[i] = scanType

			// Nullable
			nullable, ok := tp.Nullable()
			if !ok {
				t.Errorf("nullable not ok %q", name)
				continue
			}
			if nullable != column.nullable {
				t.Errorf("nullable mismatch for column %q: %t != %t", name, nullable, column.nullable)
			}

			// Length
			// length, ok := tp.Length()
			// if length != column.length {
			// 	if !ok {
			// 		t.Errorf("length not ok for column %q", name)
			// 	} else {
			// 		t.Errorf("length mismatch for column %q: %d != %d", name, length, column.length)
			// 	}
			// 	continue
			// }

			// Precision and Scale
			precision, scale, ok := tp.DecimalSize()
			if precision != column.precision {
				if !ok {
					t.Errorf("precision not ok for column %q", name)
				} else {
					t.Errorf("precision mismatch for column %q: %d != %d", name, precision, column.precision)
				}
				continue
			}
			if scale != column.scale {
				if !ok {
					t.Errorf("scale not ok for column %q", name)
				} else {
					t.Errorf("scale mismatch for column %q: %d != %d", name, scale, column.scale)
				}
				continue
			}
		}

		values := make([]interface{}, len(tt))
		for i := range values {
			values[i] = reflect.New(types[i]).Interface()
		}
		i := 0
		for rows.Next() {
			err = rows.Scan(values...)
			if err != nil {
				t.Fatalf("failed to scan values in %v", err)
			}
			for j := range values {
				value := reflect.ValueOf(values[j]).Elem().Interface()
				if !reflect.DeepEqual(value, columns[j].valuesOut[i]) {
					if columns[j].scanType == scanTypeRawBytes {
						t.Errorf("row %d, column %d: %v != %v", i, j, string(value.(sql.RawBytes)), string(columns[j].valuesOut[i].(sql.RawBytes)))
					} else {
						t.Errorf("row %d, column %d: %v != %v", i, j, value, columns[j].valuesOut[i])
					}
				}
			}
			i++
		}
		if i != 3 {
			t.Errorf("expected 3 rows, got %d", i)
		}

		if err := rows.Close(); err != nil {
			t.Errorf("error closing rows: %s", err)
		}
	})
}

func TestValuerWithValueReceiverGivenNilValue(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("CREATE TABLE test (value VARCHAR(255))")
		dbt.db.Exec("INSERT INTO test VALUES (?)", (*testValuer)(nil))
		// This test will panic on the INSERT if ConvertValue() does not check for typed nil before calling Value()
	})
}

// TestRawBytesAreNotModified checks for a race condition that arises when a query context
// is canceled while a user is calling rows.Scan. This is a more stringent test than the one
// proposed in https://github.com/golang/go/issues/23519. Here we're explicitly using
// `sql.RawBytes` to check the contents of our internal buffers are not modified after an implicit
// call to `Rows.Close`, so Context cancellation should **not** invalidate the backing buffers.
func TestRawBytesAreNotModified(t *testing.T) {
	const blob = "abcdefghijklmnop"
	const contextRaceIterations = 20
	const blobSize = defaultBufSize * 3 / 4 // Second row overwrites first row.
	const insertRows = 4

	var sqlBlobs = [2]string{
		strings.Repeat(blob, blobSize/len(blob)),
		strings.Repeat(strings.ToUpper(blob), blobSize/len(blob)),
	}

	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("CREATE TABLE test (id int, value BLOB) CHARACTER SET utf8")
		for i := 0; i < insertRows; i++ {
			dbt.mustExec("INSERT INTO test VALUES (?, ?)", i+1, sqlBlobs[i&1])
		}

		for i := 0; i < contextRaceIterations; i++ {
			func() {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				rows, err := dbt.db.QueryContext(ctx, `SELECT id, value FROM test`)
				if err != nil {
					t.Fatal(err)
				}

				var b int
				var raw sql.RawBytes
				for rows.Next() {
					if err := rows.Scan(&b, &raw); err != nil {
						t.Fatal(err)
					}

					before := string(raw)
					// Ensure cancelling the query does not corrupt the contents of `raw`
					cancel()
					time.Sleep(time.Microsecond * 100)
					after := string(raw)

					if before != after {
						t.Fatalf("the backing storage for sql.RawBytes has been modified (i=%v)", i)
					}
				}
				rows.Close()
			}()
		}
	})
}

var _ driver.DriverContext = &MySQLDriver{}

type dialCtxKey struct{}

func TestConnectorObeysDialTimeouts(t *testing.T) {
	if !available {
		t.Skipf("MySQL server not running on %s", netAddr)
	}

	RegisterDialContext("dialctxtest", func(ctx context.Context, addr string) (net.Conn, error) {
		var d net.Dialer
		if !ctx.Value(dialCtxKey{}).(bool) {
			return nil, fmt.Errorf("test error: query context is not propagated to our dialer")
		}
		return d.DialContext(ctx, prot, addr)
	})

	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@dialctxtest(%s)/%s?timeout=30s", user, pass, addr, dbname))
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}
	defer db.Close()

	ctx := context.WithValue(context.Background(), dialCtxKey{}, true)

	_, err = db.ExecContext(ctx, "DO 1")
	if err != nil {
		t.Fatal(err)
	}
}

func configForTests(t *testing.T) *Config {
	if !available {
		t.Skipf("MySQL server not running on %s", netAddr)
	}

	mycnf := NewConfig()
	mycnf.User = user
	mycnf.Passwd = pass
	mycnf.Addr = addr
	mycnf.Net = prot
	mycnf.DBName = dbname
	return mycnf
}

func TestNewConnector(t *testing.T) {
	mycnf := configForTests(t)
	conn, err := NewConnector(mycnf)
	if err != nil {
		t.Fatal(err)
	}

	db := sql.OpenDB(conn)
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
}

type slowConnection struct {
	net.Conn
	slowdown time.Duration
}

func (sc *slowConnection) Read(b []byte) (int, error) {
	time.Sleep(sc.slowdown)
	return sc.Conn.Read(b)
}

type connectorHijack struct {
	driver.Connector
	connErr error
}

func (cw *connectorHijack) Connect(ctx context.Context) (driver.Conn, error) {
	var conn driver.Conn
	conn, cw.connErr = cw.Connector.Connect(ctx)
	return conn, cw.connErr
}

func TestConnectorTimeoutsDuringOpen(t *testing.T) {
	RegisterDialContext("slowconn", func(ctx context.Context, addr string) (net.Conn, error) {
		var d net.Dialer
		conn, err := d.DialContext(ctx, prot, addr)
		if err != nil {
			return nil, err
		}
		return &slowConnection{Conn: conn, slowdown: 100 * time.Millisecond}, nil
	})

	mycnf := configForTests(t)
	mycnf.Net = "slowconn"

	conn, err := NewConnector(mycnf)
	if err != nil {
		t.Fatal(err)
	}

	hijack := &connectorHijack{Connector: conn}

	db := sql.OpenDB(hijack)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err = db.ExecContext(ctx, "DO 1")
	if err != context.DeadlineExceeded {
		t.Fatalf("ExecContext should have timed out")
	}
	if hijack.connErr != context.DeadlineExceeded {
		t.Fatalf("(*Connector).Connect should have timed out")
	}
}

// A connection which can only be closed.
type dummyConnection struct {
	net.Conn
	closed bool
}

func (d *dummyConnection) Close() error {
	d.closed = true
	return nil
}

func TestConnectorTimeoutsWatchCancel(t *testing.T) {
	var (
		cancel  func()           // Used to cancel the context just after connecting.
		created *dummyConnection // The created connection.
	)

	RegisterDialContext("TestConnectorTimeoutsWatchCancel", func(ctx context.Context, addr string) (net.Conn, error) {
		// Canceling at this time triggers the watchCancel error branch in Connect().
		cancel()
		created = &dummyConnection{}
		return created, nil
	})

	mycnf := NewConfig()
	mycnf.User = "root"
	mycnf.Addr = "foo"
	mycnf.Net = "TestConnectorTimeoutsWatchCancel"

	conn, err := NewConnector(mycnf)
	if err != nil {
		t.Fatal(err)
	}

	db := sql.OpenDB(conn)
	defer db.Close()

	var ctx context.Context
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	if _, err := db.Conn(ctx); err != context.Canceled {
		t.Errorf("got %v, want context.Canceled", err)
	}

	if created == nil {
		t.Fatal("no connection created")
	}
	if !created.closed {
		t.Errorf("connection not closed")
	}
}

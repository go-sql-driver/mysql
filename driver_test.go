package mysql

import (
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"strings"
	"testing"
	"time"
)

var (
	charset   string
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
	env := func(key, defaultValue string) string {
		if value := os.Getenv(key); value != "" {
			return value
		}
		return defaultValue
	}
	user := env("MYSQL_TEST_USER", "root")
	pass := env("MYSQL_TEST_PASS", "")
	prot := env("MYSQL_TEST_PROT", "tcp")
	addr := env("MYSQL_TEST_ADDR", "localhost:3306")
	dbname := env("MYSQL_TEST_DBNAME", "gotest")
	charset = "charset=utf8"
	netAddr = fmt.Sprintf("%s(%s)", prot, addr)
	dsn = fmt.Sprintf("%s:%s@%s/%s?timeout=30s&strict=true&"+charset, user, pass, netAddr, dbname)
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

func runTests(t *testing.T, name, dsn string, tests ...func(dbt *DBTest)) {
	if !available {
		t.Logf("MySQL-Server not running on %s. Skipping %s", netAddr, name)
		return
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS test")

	dbt := &DBTest{t, db}
	for _, test := range tests {
		test(dbt)
		dbt.db.Exec("DROP TABLE IF EXISTS test")
	}
}

func (dbt *DBTest) fail(method, query string, err error) {
	if len(query) > 300 {
		query = "[query too large to print]"
	}
	dbt.Fatalf("Error on %s %s: %v", method, query, err)
}

func (dbt *DBTest) mustExec(query string, args ...interface{}) (res sql.Result) {
	res, err := dbt.db.Exec(query, args...)
	if err != nil {
		dbt.fail("Exec", query, err)
	}
	return res
}

func (dbt *DBTest) mustQuery(query string, args ...interface{}) (rows *sql.Rows) {
	rows, err := dbt.db.Query(query, args...)
	if err != nil {
		dbt.fail("Query", query, err)
	}
	return rows
}

func TestCharset(t *testing.T) {
	mustSetCharset := func(charsetParam, expected string) {
		db, err := sql.Open("mysql", strings.Replace(dsn, charset, charsetParam, 1))
		if err != nil {
			t.Fatalf("Error on Open: %v", err)
		}
		defer db.Close()

		dbt := &DBTest{t, db}
		rows := dbt.mustQuery("SELECT @@character_set_connection")
		defer rows.Close()

		if !rows.Next() {
			dbt.Fatalf("Error getting connection charset: %v", err)
		}

		var got string
		rows.Scan(&got)

		if got != expected {
			dbt.Fatalf("Expected connection charset %s but got %s", expected, got)
		}
	}

	if !available {
		t.Logf("MySQL-Server not running on %s. Skipping TestCharset", netAddr)
		return
	}

	// non utf8 test
	mustSetCharset("charset=ascii", "ascii")

	// when the first charset is invalid, use the second
	mustSetCharset("charset=none,utf8", "utf8")

	// when the first charset is valid, use it
	mustSetCharset("charset=ascii,utf8", "ascii")
	mustSetCharset("charset=utf8,ascii", "utf8")
}

func TestFailingCharset(t *testing.T) {
	if !available {
		t.Logf("MySQL-Server not running on %s. Skipping TestFailingCharset", netAddr)
		return
	}
	db, err := sql.Open("mysql", strings.Replace(dsn, charset, "charset=none", 1))
	if err != nil {
		t.Fatalf("Error on Open: %v", err)
	}
	defer db.Close()

	// run query to really establish connection...
	_, err = db.Exec("SELECT 1")
	if err == nil {
		db.Close()
		t.Fatalf("Connection must not succeed without a valid charset")
	}
}

func TestRawBytesResultExceedsBuffer(t *testing.T) {
	runTests(t, "TestRawBytesResultExceedsBuffer", dsn, func(dbt *DBTest) {
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

func TestCRUD(t *testing.T) {
	runTests(t, "TestCRUD", dsn, func(dbt *DBTest) {
		// Create Table
		dbt.mustExec("CREATE TABLE test (value BOOL)")

		// Test for unexpected data
		var out bool
		rows := dbt.mustQuery("SELECT * FROM test")
		if rows.Next() {
			dbt.Error("unexpected data in empty table")
		}

		// Create Data
		res := dbt.mustExec("INSERT INTO test VALUES (1)")
		count, err := res.RowsAffected()
		if err != nil {
			dbt.Fatalf("res.RowsAffected() returned error: %v", err)
		}
		if count != 1 {
			dbt.Fatalf("Expected 1 affected row, got %d", count)
		}

		id, err := res.LastInsertId()
		if err != nil {
			dbt.Fatalf("res.LastInsertId() returned error: %v", err)
		}
		if id != 0 {
			dbt.Fatalf("Expected InsertID 0, got %d", id)
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

		// Update
		res = dbt.mustExec("UPDATE test SET value = ? WHERE value = ?", false, true)
		count, err = res.RowsAffected()
		if err != nil {
			dbt.Fatalf("res.RowsAffected() returned error: %v", err)
		}
		if count != 1 {
			dbt.Fatalf("Expected 1 affected row, got %d", count)
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

		// Delete
		res = dbt.mustExec("DELETE FROM test WHERE value = ?", false)
		count, err = res.RowsAffected()
		if err != nil {
			dbt.Fatalf("res.RowsAffected() returned error: %v", err)
		}
		if count != 1 {
			dbt.Fatalf("Expected 1 affected row, got %d", count)
		}

		// Check for unexpected rows
		res = dbt.mustExec("DELETE FROM test")
		count, err = res.RowsAffected()
		if err != nil {
			dbt.Fatalf("res.RowsAffected() returned error: %v", err)
		}
		if count != 0 {
			dbt.Fatalf("Expected 0 affected row, got %d", count)
		}
	})
}

func TestInt(t *testing.T) {
	runTests(t, "TestInt", dsn, func(dbt *DBTest) {
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

			dbt.mustExec("DROP TABLE IF EXISTS test")
		}
	})
}

func TestFloat(t *testing.T) {
	runTests(t, "TestFloat", dsn, func(dbt *DBTest) {
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
			dbt.mustExec("DROP TABLE IF EXISTS test")
		}
	})
}

func TestString(t *testing.T) {
	runTests(t, "TestString", dsn, func(dbt *DBTest) {
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
			dbt.Fatalf("Error on BLOB-Query: %v", err)
		} else if out != in {
			dbt.Errorf("BLOB: %s != %s", in, out)
		}
	})
}

func TestDateTime(t *testing.T) {
	type testmode struct {
		selectSuffix string
		args         []interface{}
	}
	type timetest struct {
		in      interface{}
		sOut    string
		tOut    time.Time
		tIsZero bool
	}
	type tester func(dbt *DBTest, rows *sql.Rows,
		test *timetest, sqltype, resulttype, mode string)
	type setup struct {
		vartype   string
		dsnSuffix string
		test      tester
	}
	var (
		modes = map[string]*testmode{
			"text":   &testmode{},
			"binary": &testmode{" WHERE 1 = ?", []interface{}{1}},
		}
		timetests = map[string][]*timetest{
			"DATE": {
				{sDate, sDate, tDate, false},
				{sDate0, sDate0, tDate0, true},
				{tDate, sDate, tDate, false},
				{tDate0, sDate0, tDate0, true},
			},
			"DATETIME": {
				{sDateTime, sDateTime, tDateTime, false},
				{sDateTime0, sDateTime0, tDate0, true},
				{tDateTime, sDateTime, tDateTime, false},
				{tDate0, sDateTime0, tDate0, true},
			},
		}
		setups = []*setup{
			{"string", "&parseTime=false", func(
				dbt *DBTest, rows *sql.Rows, test *timetest, sqltype, resulttype, mode string) {
				var sOut string
				if err := rows.Scan(&sOut); err != nil {
					dbt.Errorf("%s (%s %s): %v", sqltype, resulttype, mode, err)
				} else if test.sOut != sOut {
					dbt.Errorf("%s (%s %s): %s != %s", sqltype, resulttype, mode, test.sOut, sOut)
				}
			}},
			{"time.Time", "&parseTime=true", func(
				dbt *DBTest, rows *sql.Rows, test *timetest, sqltype, resulttype, mode string) {
				var tOut time.Time
				if err := rows.Scan(&tOut); err != nil {
					dbt.Errorf("%s (%s %s): %v", sqltype, resulttype, mode, err)
				} else if test.tOut != tOut || test.tIsZero != tOut.IsZero() {
					dbt.Errorf("%s (%s %s): %s [%t] != %s [%t]", sqltype, resulttype, mode, test.tOut, test.tIsZero, tOut, tOut.IsZero())
				}
			}},
		}
	)

	var s *setup
	testTime := func(dbt *DBTest) {
		var rows *sql.Rows
		for sqltype, tests := range timetests {
			dbt.mustExec("CREATE TABLE test (value " + sqltype + ")")
			for _, test := range tests {
				for mode, q := range modes {
					dbt.mustExec("TRUNCATE test")
					dbt.mustExec("INSERT INTO test VALUES (?)", test.in)
					rows = dbt.mustQuery("SELECT value FROM test"+q.selectSuffix, q.args...)
					if rows.Next() {
						s.test(dbt, rows, test, sqltype, s.vartype, mode)
					} else {
						if err := rows.Err(); err != nil {
							dbt.Errorf("%s (%s %s): %v",
								sqltype, s.vartype, mode, err)
						} else {
							dbt.Errorf("%s (%s %s): no data",
								sqltype, s.vartype, mode)
						}
					}
				}
			}
			dbt.mustExec("DROP TABLE IF EXISTS test")
		}
	}

	timeDsn := dsn + "&sql_mode=ALLOW_INVALID_DATES"
	for _, v := range setups {
		s = v
		runTests(t, "TestDateTime", timeDsn+s.dsnSuffix, testTime)
	}
}

func TestNULL(t *testing.T) {
	runTests(t, "TestNULL", dsn, func(dbt *DBTest) {
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
		err = nullStmt.QueryRow().Scan(&nb)
		if err != nil {
			dbt.Fatal(err)
		}
		if nb.Valid {
			dbt.Error("Valid NullBool which should be invalid")
		}
		// Valid
		err = nonNullStmt.QueryRow().Scan(&nb)
		if err != nil {
			dbt.Fatal(err)
		}
		if !nb.Valid {
			dbt.Error("Invalid NullBool which should be valid")
		} else if nb.Bool != true {
			dbt.Errorf("Unexpected NullBool value: %t (should be true)", nb.Bool)
		}

		// NullFloat64
		var nf sql.NullFloat64
		// Invalid
		err = nullStmt.QueryRow().Scan(&nf)
		if err != nil {
			dbt.Fatal(err)
		}
		if nf.Valid {
			dbt.Error("Valid NullFloat64 which should be invalid")
		}
		// Valid
		err = nonNullStmt.QueryRow().Scan(&nf)
		if err != nil {
			dbt.Fatal(err)
		}
		if !nf.Valid {
			dbt.Error("Invalid NullFloat64 which should be valid")
		} else if nf.Float64 != float64(1) {
			dbt.Errorf("Unexpected NullFloat64 value: %f (should be 1.0)", nf.Float64)
		}

		// NullInt64
		var ni sql.NullInt64
		// Invalid
		err = nullStmt.QueryRow().Scan(&ni)
		if err != nil {
			dbt.Fatal(err)
		}
		if ni.Valid {
			dbt.Error("Valid NullInt64 which should be invalid")
		}
		// Valid
		err = nonNullStmt.QueryRow().Scan(&ni)
		if err != nil {
			dbt.Fatal(err)
		}
		if !ni.Valid {
			dbt.Error("Invalid NullInt64 which should be valid")
		} else if ni.Int64 != int64(1) {
			dbt.Errorf("Unexpected NullInt64 value: %d (should be 1)", ni.Int64)
		}

		// NullString
		var ns sql.NullString
		// Invalid
		err = nullStmt.QueryRow().Scan(&ns)
		if err != nil {
			dbt.Fatal(err)
		}
		if ns.Valid {
			dbt.Error("Valid NullString which should be invalid")
		}
		// Valid
		err = nonNullStmt.QueryRow().Scan(&ns)
		if err != nil {
			dbt.Fatal(err)
		}
		if !ns.Valid {
			dbt.Error("Invalid NullString which should be valid")
		} else if ns.String != `1` {
			dbt.Error("Unexpected NullString value:" + ns.String + " (should be `1`)")
		}

		// Insert NULL
		dbt.mustExec("CREATE TABLE test (dummmy1 int, value int, dummy2 int)")

		dbt.mustExec("INSERT INTO test VALUES (?, ?, ?)", 1, nil, 2)

		var out interface{}
		rows := dbt.mustQuery("SELECT * FROM test")
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

func TestLongData(t *testing.T) {
	runTests(t, "TestLongData", dsn, func(dbt *DBTest) {
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

		in := strings.Repeat(`0`, maxAllowedPacketSize+1)
		var out string
		var rows *sql.Rows

		// Long text data
		const nonDataQueryLen = 28 // length query w/o value
		inS := in[:maxAllowedPacketSize-nonDataQueryLen]
		dbt.mustExec("INSERT INTO test VALUES('" + inS + "')")
		rows = dbt.mustQuery("SELECT value FROM test")
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
		if rows.Next() {
			rows.Scan(&out)
			if in != out {
				dbt.Fatalf("LONGBLOB: length in: %d, length out: %d", len(in), len(out))
			}
			if rows.Next() {
				dbt.Error("LONGBLOB: unexpexted row")
			}
		} else {
			dbt.Fatalf("LONGBLOB: no data")
		}
	})
}

func TestLoadData(t *testing.T) {
	runTests(t, "TestLoadData", dsn, func(dbt *DBTest) {
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
					dbt.Fatalf("%s != %s", values[i-1], value)
				}
			}
			err = rows.Err()
			if err != nil {
				dbt.Fatal(err.Error())
			}

			if i != 4 {
				dbt.Fatalf("Rows count mismatch. Got %d, want 4", i)
			}
		}
		file, err := ioutil.TempFile("", "gotest")
		defer os.Remove(file.Name())
		if err != nil {
			dbt.Fatal(err)
		}
		file.WriteString("1\ta string\n2\ta string containing a \\t\n3\ta string containing a \\n\n4\ta string containing both \\t\\n\n")
		file.Close()

		dbt.db.Exec("DROP TABLE IF EXISTS test")
		dbt.mustExec("CREATE TABLE test (id INT NOT NULL PRIMARY KEY, value TEXT NOT NULL) CHARACTER SET utf8")

		// Local File
		RegisterLocalFile(file.Name())
		dbt.mustExec(fmt.Sprintf("LOAD DATA LOCAL INFILE '%q' INTO TABLE test", file.Name()))
		verifyLoadDataResult()
		// negative test
		_, err = dbt.db.Exec("LOAD DATA LOCAL INFILE 'doesnotexist' INTO TABLE test")
		if err == nil {
			dbt.Fatal("Load non-existent file didn't fail")
		} else if err.Error() != "Local File 'doesnotexist' is not registered. Use the DSN parameter 'allowAllFiles=true' to allow all files" {
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
			dbt.Fatal("Load non-existent Reader didn't fail")
		} else if err.Error() != "Reader 'doesnotexist' is not registered" {
			dbt.Fatal(err.Error())
		}
	})
}

func TestStrict(t *testing.T) {
	// ALLOW_INVALID_DATES to get rid of stricter modes - we want to test for warnings, not errors
	relaxedDsn := dsn + "&sql_mode=ALLOW_INVALID_DATES"
	runTests(t, "TestStrict", relaxedDsn, func(dbt *DBTest) {
		dbt.mustExec("CREATE TABLE test (a TINYINT NOT NULL, b CHAR(4))")

		var queries = [...]struct {
			in    string
			codes []string
		}{
			{"DROP TABLE IF EXISTS no_such_table", []string{"1051"}},
			{"INSERT INTO test VALUES(10,'mysql'),(NULL,'test'),(300,'Open Source')", []string{"1265", "1048", "1264", "1265"}},
		}
		var err error

		var checkWarnings = func(err error, mode string, idx int) {
			if err == nil {
				dbt.Errorf("Expected STRICT error on query [%s] %s", mode, queries[idx].in)
			}

			if warnings, ok := err.(MySQLWarnings); ok {
				var codes = make([]string, len(warnings))
				for i := range warnings {
					codes[i] = warnings[i].Code
				}
				if len(codes) != len(queries[idx].codes) {
					dbt.Errorf("Unexpected STRICT error count on query [%s] %s: Wanted %v, Got %v", mode, queries[idx].in, queries[idx].codes, codes)
				}

				for i := range warnings {
					if codes[i] != queries[idx].codes[i] {
						dbt.Errorf("Unexpected STRICT error codes on query [%s] %s: Wanted %v, Got %v", mode, queries[idx].in, queries[idx].codes, codes)
						return
					}
				}

			} else {
				dbt.Errorf("Unexpected error on query [%s] %s: %s", mode, queries[idx].in, err.Error())
			}
		}

		// text protocol
		for i := range queries {
			_, err = dbt.db.Exec(queries[i].in)
			checkWarnings(err, "text", i)
		}

		var stmt *sql.Stmt

		// binary protocol
		for i := range queries {
			stmt, err = dbt.db.Prepare(queries[i].in)
			if err != nil {
				dbt.Error("Error on preparing query %: ", queries[i].in, err.Error())
			}

			_, err = stmt.Exec()
			checkWarnings(err, "binary", i)

			err = stmt.Close()
			if err != nil {
				dbt.Error("Error on closing stmt for query %: ", queries[i].in, err.Error())
			}
		}
	})
}

// Special cases

func TestRowsClose(t *testing.T) {
	runTests(t, "TestRowsClose", dsn, func(dbt *DBTest) {
		rows, err := dbt.db.Query("SELECT 1")
		if err != nil {
			dbt.Fatal(err)
		}

		err = rows.Close()
		if err != nil {
			dbt.Fatal(err)
		}

		if rows.Next() {
			dbt.Fatal("Unexpected row after rows.Close()")
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
	runTests(t, "TestCloseStmtBeforeRows", dsn, func(dbt *DBTest) {
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
			dbt.Fatal("Getting row failed")
		} else {
			err = rows.Err()
			if err != nil {
				dbt.Fatal(err)
			}

			var out bool
			err = rows.Scan(&out)
			if err != nil {
				dbt.Fatalf("Error on rows.Scan(): %v", err)
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
	runTests(t, "TestStmtMultiRows", dsn, func(dbt *DBTest) {
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
			dbt.Fatal("1st rows1.Next failed")
		} else {
			err = rows1.Err()
			if err != nil {
				dbt.Fatal(err)
			}

			err = rows1.Scan(&out)
			if err != nil {
				dbt.Fatalf("Error on rows.Scan(): %v", err)
			}
			if out != true {
				dbt.Errorf("true != %t", out)
			}
		}

		if !rows2.Next() {
			dbt.Fatal("1st rows2.Next failed")
		} else {
			err = rows2.Err()
			if err != nil {
				dbt.Fatal(err)
			}

			err = rows2.Scan(&out)
			if err != nil {
				dbt.Fatalf("Error on rows.Scan(): %v", err)
			}
			if out != true {
				dbt.Errorf("true != %t", out)
			}
		}

		// 2
		if !rows1.Next() {
			dbt.Fatal("2nd rows1.Next failed")
		} else {
			err = rows1.Err()
			if err != nil {
				dbt.Fatal(err)
			}

			err = rows1.Scan(&out)
			if err != nil {
				dbt.Fatalf("Error on rows.Scan(): %v", err)
			}
			if out != false {
				dbt.Errorf("false != %t", out)
			}

			if rows1.Next() {
				dbt.Fatal("Unexpected row on rows1")
			}
			err = rows1.Close()
			if err != nil {
				dbt.Fatal(err)
			}
		}

		if !rows2.Next() {
			dbt.Fatal("2nd rows2.Next failed")
		} else {
			err = rows2.Err()
			if err != nil {
				dbt.Fatal(err)
			}

			err = rows2.Scan(&out)
			if err != nil {
				dbt.Fatalf("Error on rows.Scan(): %v", err)
			}
			if out != false {
				dbt.Errorf("false != %t", out)
			}

			if rows2.Next() {
				dbt.Fatal("Unexpected row on rows2")
			}
			err = rows2.Close()
			if err != nil {
				dbt.Fatal(err)
			}
		}
	})
}

func TestConcurrent(t *testing.T) {
	if readBool(os.Getenv("MYSQL_TEST_CONCURRENT")) != true {
		t.Log("CONCURRENT env var not set. Skipping TestConcurrent")
		return
	}
	runTests(t, "TestConcurrent", dsn, func(dbt *DBTest) {
		var max int
		err := dbt.db.QueryRow("SELECT @@max_connections").Scan(&max)
		if err != nil {
			dbt.Fatalf("%v", err)
		}
		dbt.Logf("Testing up to %d concurrent connections \r\n", max)
		canStop := false
		c := make(chan struct{}, max)
		for i := 0; i < max; i++ {
			go func(id int) {
				tx, err := dbt.db.Begin()
				if err != nil {
					canStop = true
					if err.Error() == "Error 1040: Too many connections" {
						max--
						return
					} else {
						dbt.Fatalf("Error on Con %d: %s", id, err.Error())
					}
				}
				c <- struct{}{}
				for !canStop {
					_, err = tx.Exec("SELECT 1")
					if err != nil {
						canStop = true
						dbt.Fatalf("Error on Con %d: %s", id, err.Error())
					}
				}
				err = tx.Commit()
				if err != nil {
					canStop = true
					dbt.Fatalf("Error on Con %d: %s", id, err.Error())
				}
			}(i)
		}
		for i := 0; i < max; i++ {
			<-c
		}
		canStop = true

		dbt.Logf("Reached %d concurrent connections \r\n", max)
	})
}

// BENCHMARKS
var sample []byte

func initBenchmarks() ([]byte, int, int) {
	if sample == nil {
		sample = []byte(strings.Repeat("0123456789abcdef", 1024*1024))
	}
	return sample, 16, len(sample)
}

func BenchmarkRoundtripText(b *testing.B) {
	sample, min, max := initBenchmarks()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		b.Fatalf("crashed")
	}
	defer db.Close()
	var result string
	for i := 0; i < b.N; i++ {
		length := min + i
		if length > max {
			length = max
		}
		test := string(sample[0:length])
		rows, err := db.Query("SELECT \"" + test + "\"")
		if err != nil {
			b.Fatalf("crashed")
		}
		if !rows.Next() {
			rows.Close()
			b.Fatalf("crashed")
		}
		err = rows.Scan(&result)
		if err != nil {
			rows.Close()
			b.Fatalf("crashed")
		}
		if result != test {
			rows.Close()
			b.Errorf("mismatch")
		}
		rows.Close()
	}
}

func BenchmarkRoundtripPrepared(b *testing.B) {
	sample, min, max := initBenchmarks()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		b.Fatalf("crashed")
	}
	defer db.Close()
	var result string
	stmt, err := db.Prepare("SELECT ?")
	if err != nil {
		b.Fatalf("crashed")
	}
	for i := 0; i < b.N; i++ {
		length := min + i
		if length > max {
			length = max
		}
		test := string(sample[0:length])
		rows, err := stmt.Query(test)
		if err != nil {
			b.Fatalf("crashed")
		}
		if !rows.Next() {
			rows.Close()
			b.Fatalf("crashed")
		}
		err = rows.Scan(&result)
		if err != nil {
			rows.Close()
			b.Fatalf("crashed")
		}
		if result != test {
			rows.Close()
			b.Errorf("mismatch")
		}
		rows.Close()
	}
}

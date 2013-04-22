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
	dsn = fmt.Sprintf("%s:%s@%s/%s?timeout=30s&"+charset, user, pass, netAddr, dbname)
	c, err := net.Dial(prot, addr)
	if err == nil {
		available = true
		c.Close()
	}
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

type DBTest struct {
	*testing.T
	*sql.DB
}

func runTests(t *testing.T, name string, tests ...func(db *DBTest)) {
	if !available {
		t.Logf("MySQL-Server not running on %s. Skipping %s", netAddr, name)
		return
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}
	defer db.Close()

	environment := &DBTest{t, db}
	for _, test := range tests {
		test(environment)
	}
}

func args(args ...interface{}) []interface{} {
	return args
}

func (dbt *DBTest) fail(method, query string, err error) {
	if len(query) > 300 {
		query = "[query too large to print]"
	}
	dbt.Fatalf("Error on %s %s: %v", method, query, err)
}

func (dbt *DBTest) execWithArgs(query string, args []interface{}, test func(res sql.Result)) {
	res, err := dbt.Exec(query, args...)
	if err != nil {
		dbt.fail("Exec", query, err)
	}
	test(res)
}

func (dbt *DBTest) exec(query string, test func(result sql.Result)) {
	dbt.execWithArgs(query, args(), test)
}

func (dbt *DBTest) queryWithArgs(query string, args []interface{}, test func(rows *sql.Rows)) {
	rows, err := dbt.Query(query, args...)
	if err != nil {
		dbt.fail("Query", query, err)
	}
	defer rows.Close()
	test(rows)
}

func (dbt *DBTest) query(query string, test func(result *sql.Rows)) {
	dbt.queryWithArgs(query, args(), test)
}

func (dbt *DBTest) mustExec(query string, args ...interface{}) (res sql.Result) {
	res, err := dbt.Exec(query, args...)
	if err != nil {
		if len(query) > 300 {
			query = "[query too large to print]"
		}
		dbt.Fatalf("Error on Exec %s: %v", query, err)
	}
	return res
}

func (dbt *DBTest) mustQuery(query string, args ...interface{}) (rows *sql.Rows) {
	rows, err := dbt.Query(query, args...)
	if err != nil {
		if len(query) > 300 {
			query = "[query too large to print]"
		}
		dbt.Fatalf("Error on Query %s: %v", query, err)
	}
	return rows
}

func TestRawBytesResultExceedsBuffer(t *testing.T) {
	runTests(t, "TestRawBytesResultExceedsBuffer", _rawBytesResultExceedsBuffer)
}

func TestCRUD(t *testing.T) {
	runTests(t, "TestCRUD", _crud)
}

func _rawBytesResultExceedsBuffer(db *DBTest) {
	// defaultBufSize from buffer.go
	expected := strings.Repeat("abc", defaultBufSize)
	db.query("SELECT '"+expected+"'", func(rows *sql.Rows) {
		if !rows.Next() {
			db.Error("expected result, got none")
		}
		var result sql.RawBytes
		rows.Scan(&result)
		if expected != string(result) {
			db.Error("result did not match expected value")
		}
	})
}

func _crud(db *DBTest) {
	db.mustExec("DROP TABLE IF EXISTS test")

	// Create Table
	db.mustExec("CREATE TABLE test (value BOOL)")

	// Test for unexpected data
	var out bool
	rows := db.mustQuery("SELECT * FROM test")
	if rows.Next() {
		db.Error("unexpected data in empty table")
	}

	// Create Data
	res := db.mustExec("INSERT INTO test VALUES (1)")
	count, err := res.RowsAffected()
	if err != nil {
		db.Fatalf("res.RowsAffected() returned error: %v", err)
	}
	if count != 1 {
		db.Fatalf("Expected 1 affected row, got %d", count)
	}

	id, err := res.LastInsertId()
	if err != nil {
		db.Fatalf("res.LastInsertId() returned error: %v", err)
	}
	if id != 0 {
		db.Fatalf("Expected InsertID 0, got %d", id)
	}

	// Read
	rows = db.mustQuery("SELECT value FROM test")
	if rows.Next() {
		rows.Scan(&out)
		if true != out {
			db.Errorf("true != %t", out)
		}

		if rows.Next() {
			db.Error("unexpected data")
		}
	} else {
		db.Error("no data")
	}

	// Update
	res = db.mustExec("UPDATE test SET value = ? WHERE value = ?", false, true)
	count, err = res.RowsAffected()
	if err != nil {
		db.Fatalf("res.RowsAffected() returned error: %v", err)
	}
	if count != 1 {
		db.Fatalf("Expected 1 affected row, got %d", count)
	}

	// Check Update
	rows = db.mustQuery("SELECT value FROM test")
	if rows.Next() {
		rows.Scan(&out)
		if false != out {
			db.Errorf("false != %t", out)
		}

		if rows.Next() {
			db.Error("unexpected data")
		}
	} else {
		db.Error("no data")
	}

	// Delete
	res = db.mustExec("DELETE FROM test WHERE value = ?", false)
	count, err = res.RowsAffected()
	if err != nil {
		db.Fatalf("res.RowsAffected() returned error: %v", err)
	}
	if count != 1 {
		db.Fatalf("Expected 1 affected row, got %d", count)
	}

	// Check for unexpected rows
	res = db.mustExec("DELETE FROM test")
	count, err = res.RowsAffected()
	if err != nil {
		db.Fatalf("res.RowsAffected() returned error: %v", err)
	}
	if count != 0 {
		db.Fatalf("Expected 0 affected row, got %d", count)
	}
}

func TestInt(t *testing.T) {
	runTests(t, "TestInt", _int)
}

func _int(db *DBTest) {
	db.mustExec("DROP TABLE IF EXISTS test")

	types := [5]string{"TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT"}
	in := int64(42)
	var out int64
	var rows *sql.Rows

	// SIGNED
	for _, v := range types {
		db.mustExec("CREATE TABLE test (value " + v + ")")

		db.mustExec("INSERT INTO test VALUES (?)", in)

		rows = db.mustQuery("SELECT value FROM test")
		if rows.Next() {
			rows.Scan(&out)
			if in != out {
				db.Errorf("%s: %d != %d", v, in, out)
			}
		} else {
			db.Errorf("%s: no data", v)
		}

		db.mustExec("DROP TABLE IF EXISTS test")
	}

	// UNSIGNED ZEROFILL
	for _, v := range types {
		db.mustExec("CREATE TABLE test (value " + v + " ZEROFILL)")

		db.mustExec("INSERT INTO test VALUES (?)", in)

		rows = db.mustQuery("SELECT value FROM test")
		if rows.Next() {
			rows.Scan(&out)
			if in != out {
				db.Errorf("%s ZEROFILL: %d != %d", v, in, out)
			}
		} else {
			db.Errorf("%s ZEROFILL: no data", v)
		}

		db.mustExec("DROP TABLE IF EXISTS test")
	}
}

func TestFloat(t *testing.T) {
	runTests(t, "TestFloat", _float)
}

func _float(db *DBTest) {
	db.mustExec("DROP TABLE IF EXISTS test")

	types := [2]string{"FLOAT", "DOUBLE"}
	in := float32(42.23)
	var out float32
	var rows *sql.Rows

	for _, v := range types {
		db.mustExec("CREATE TABLE test (value " + v + ")")

		db.mustExec("INSERT INTO test VALUES (?)", in)

		rows = db.mustQuery("SELECT value FROM test")
		if rows.Next() {
			rows.Scan(&out)
			if in != out {
				db.Errorf("%s: %g != %g", v, in, out)
			}
		} else {
			db.Errorf("%s: no data", v)
		}

		db.mustExec("DROP TABLE IF EXISTS test")
	}
}

func TestString(t *testing.T) {
	runTests(t, "TestString", _string)
}

func _string(db *DBTest) {
	db.mustExec("DROP TABLE IF EXISTS test")

	types := [6]string{"CHAR(255)", "VARCHAR(255)", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT"}
	in := "κόσμε üöäßñóùéàâÿœ'îë Árvíztűrő いろはにほへとちりぬるを イロハニホヘト דג סקרן чащах  น่าฟังเอย"
	var out string
	var rows *sql.Rows

	for _, v := range types {
		db.mustExec("CREATE TABLE test (value " + v + ") CHARACTER SET utf8 COLLATE utf8_unicode_ci")

		db.mustExec("INSERT INTO test VALUES (?)", in)

		rows = db.mustQuery("SELECT value FROM test")
		if rows.Next() {
			rows.Scan(&out)
			if in != out {
				db.Errorf("%s: %s != %s", v, in, out)
			}
		} else {
			db.Errorf("%s: no data", v)
		}

		db.mustExec("DROP TABLE IF EXISTS test")
	}

	// BLOB
	db.mustExec("CREATE TABLE test (id int, value BLOB) CHARACTER SET utf8 COLLATE utf8_unicode_ci")

	id := 2
	in = "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, " +
		"sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, " +
		"sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. " +
		"Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. " +
		"Lorem ipsum dolor sit amet, consetetur sadipscing elitr, " +
		"sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, " +
		"sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. " +
		"Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet."
	db.mustExec("INSERT INTO test VALUES (?, ?)", id, in)

	err := db.QueryRow("SELECT value FROM test WHERE id = ?", id).Scan(&out)
	if err != nil {
		db.Fatalf("Error on BLOB-Query: %v", err)
	} else if out != in {
		db.Errorf("BLOB: %s != %s", in, out)
	}
}

func TestDateTime(t *testing.T) {
	var modes = [2]string{"text", "binary"}
	var types = [2]string{"DATE", "DATETIME"}
	var tests = [2][]struct {
		in      interface{}
		sOut    string
		tOut    time.Time
		tIsZero bool
	}{
		{
			{"2012-06-14", "2012-06-14", time.Date(2012, 6, 14, 0, 0, 0, 0, time.UTC), false},
			{"0000-00-00", "0000-00-00", time.Time{}, true},
			{time.Date(2012, 6, 14, 0, 0, 0, 0, time.UTC), "2012-06-14", time.Date(2012, 6, 14, 0, 0, 0, 0, time.UTC), false},
			{time.Time{}, "0000-00-00", time.Time{}, true},
		},
		{
			{"2011-11-20 21:27:37", "2011-11-20 21:27:37", time.Date(2011, 11, 20, 21, 27, 37, 0, time.UTC), false},
			{"0000-00-00 00:00:00", "0000-00-00 00:00:00", time.Time{}, true},
			{time.Date(2011, 11, 20, 21, 27, 37, 0, time.UTC), "2011-11-20 21:27:37", time.Date(2011, 11, 20, 21, 27, 37, 0, time.UTC), false},
			{time.Time{}, "0000-00-00 00:00:00", time.Time{}, true},
		},
	}
	var sOut string
	var tOut time.Time

	var rows [2]*sql.Rows
	var err error
	var resultType string

	testTime := func(db *DBTest) {
		defer db.mustExec("DROP TABLE IF EXISTS test")
		for i, v := range types {
			db.mustExec("DROP TABLE IF EXISTS test")
			db.mustExec("CREATE TABLE test (value " + v + ") CHARACTER SET utf8 COLLATE utf8_unicode_ci")
			for j := range tests[i] {
				db.mustExec("INSERT INTO test VALUES (?)", tests[i][j].in)
				// string
				rows[0] = db.mustQuery("SELECT value FROM test")                // text
				rows[1] = db.mustQuery("SELECT value FROM test WHERE 1 = ?", 1) // binary

				for k := range rows {
					if rows[k].Next() {
						if resultType == "string" {
							err = rows[k].Scan(&sOut)
							if err != nil {
								db.Errorf("%s (%s %s): %v",
									v, resultType, modes[k], err)
							} else if tests[i][j].sOut != sOut {
								db.Errorf("%s (%s %s): %s != %s",
									v, resultType, modes[k],
									tests[i][j].sOut, sOut)
							}
						} else {
							err = rows[k].Scan(&tOut)
							if err != nil {
								t.Errorf("%s (%s %s): %v",
									v, resultType, modes[k], err)
							} else if tests[i][j].tOut != tOut || tests[i][j].tIsZero != tOut.IsZero() {
								t.Errorf("%s (%s %s): %s [%t] != %s [%t]",
									v, resultType, modes[k],
									tests[i][j].tOut, tests[i][j].tIsZero,
									tOut, tOut.IsZero())
							}
						}
					} else {
						err = rows[k].Err()
						if err != nil {
							db.Errorf("%s (%s %s): %v", v, resultType, modes[k], err)
						} else {
							db.Errorf("%s (%s %s): no data", v, resultType, modes[k])
						}
					}
				}
			}
		}
	}

	resultType = "string"
	oldDsn := dsn
	dsn += "&sql_mode=ALLOW_INVALID_DATES"
	runTests(t, "TestDateTime", testTime)
	dsn += "&parseTime=true"
	resultType = "time.Time"
	runTests(t, "TestDateTime", testTime)
	dsn = oldDsn
}

func TestNULL(t *testing.T) {
	runTests(t, "TestNULL", _null)
}

func _null(db *DBTest) {
	nullStmt, err := db.Prepare("SELECT NULL")
	if err != nil {
		db.Fatal(err)
	}
	defer nullStmt.Close()

	nonNullStmt, err := db.Prepare("SELECT 1")
	if err != nil {
		db.Fatal(err)
	}
	defer nonNullStmt.Close()

	// NullBool
	var nb sql.NullBool
	// Invalid
	err = nullStmt.QueryRow().Scan(&nb)
	if err != nil {
		db.Fatal(err)
	}
	if nb.Valid {
		db.Error("Valid NullBool which should be invalid")
	}
	// Valid
	err = nonNullStmt.QueryRow().Scan(&nb)
	if err != nil {
		db.Fatal(err)
	}
	if !nb.Valid {
		db.Error("Invalid NullBool which should be valid")
	} else if nb.Bool != true {
		db.Errorf("Unexpected NullBool value: %t (should be true)", nb.Bool)
	}

	// NullFloat64
	var nf sql.NullFloat64
	// Invalid
	err = nullStmt.QueryRow().Scan(&nf)
	if err != nil {
		db.Fatal(err)
	}
	if nf.Valid {
		db.Error("Valid NullFloat64 which should be invalid")
	}
	// Valid
	err = nonNullStmt.QueryRow().Scan(&nf)
	if err != nil {
		db.Fatal(err)
	}
	if !nf.Valid {
		db.Error("Invalid NullFloat64 which should be valid")
	} else if nf.Float64 != float64(1) {
		db.Errorf("Unexpected NullFloat64 value: %f (should be 1.0)", nf.Float64)
	}

	// NullInt64
	var ni sql.NullInt64
	// Invalid
	err = nullStmt.QueryRow().Scan(&ni)
	if err != nil {
		db.Fatal(err)
	}
	if ni.Valid {
		db.Error("Valid NullInt64 which should be invalid")
	}
	// Valid
	err = nonNullStmt.QueryRow().Scan(&ni)
	if err != nil {
		db.Fatal(err)
	}
	if !ni.Valid {
		db.Error("Invalid NullInt64 which should be valid")
	} else if ni.Int64 != int64(1) {
		db.Errorf("Unexpected NullInt64 value: %d (should be 1)", ni.Int64)
	}

	// NullString
	var ns sql.NullString
	// Invalid
	err = nullStmt.QueryRow().Scan(&ns)
	if err != nil {
		db.Fatal(err)
	}
	if ns.Valid {
		db.Error("Valid NullString which should be invalid")
	}
	// Valid
	err = nonNullStmt.QueryRow().Scan(&ns)
	if err != nil {
		db.Fatal(err)
	}
	if !ns.Valid {
		db.Error("Invalid NullString which should be valid")
	} else if ns.String != `1` {
		db.Error("Unexpected NullString value:" + ns.String + " (should be `1`)")
	}

	// Insert NULL
	db.mustExec("CREATE TABLE test (dummmy1 int, value int, dummy2 int)")

	db.mustExec("INSERT INTO test VALUES (?, ?, ?)", 1, nil, 2)

	var out interface{}
	rows := db.mustQuery("SELECT * FROM test")
	if rows.Next() {
		rows.Scan(&out)
		if out != nil {
			db.Errorf("%v != nil", out)
		}
	} else {
		db.Error("no data")
	}

	db.mustExec("DROP TABLE IF EXISTS test")
}

func TestLongData(t *testing.T) {
	runTests(t, "TestLongData", _longData)
}

func _longData(db *DBTest) {
	var maxAllowedPacketSize int
	err := db.QueryRow("select @@max_allowed_packet").Scan(&maxAllowedPacketSize)
	if err != nil {
		db.Fatal(err)
	}
	maxAllowedPacketSize--

	// don't get too ambitious
	if maxAllowedPacketSize > 1<<25 {
		maxAllowedPacketSize = 1 << 25
	}

	db.mustExec("DROP TABLE IF EXISTS test")
	db.mustExec("CREATE TABLE test (value LONGBLOB) CHARACTER SET utf8 COLLATE utf8_unicode_ci")

	in := strings.Repeat(`0`, maxAllowedPacketSize+1)
	var out string
	var rows *sql.Rows

	// Long text data
	const nonDataQueryLen = 28 // length query w/o value
	inS := in[:maxAllowedPacketSize-nonDataQueryLen]
	db.mustExec("INSERT INTO test VALUES('" + inS + "')")
	rows = db.mustQuery("SELECT value FROM test")
	if rows.Next() {
		rows.Scan(&out)
		if inS != out {
			db.Fatalf("LONGBLOB: length in: %d, length out: %d", len(inS), len(out))
		}
		if rows.Next() {
			db.Error("LONGBLOB: unexpexted row")
		}
	} else {
		db.Fatalf("LONGBLOB: no data")
	}

	// Empty table
	db.mustExec("TRUNCATE TABLE test")

	// Long binary data
	db.mustExec("INSERT INTO test VALUES(?)", in)
	rows = db.mustQuery("SELECT value FROM test WHERE 1=?", 1)
	if rows.Next() {
		rows.Scan(&out)
		if in != out {
			db.Fatalf("LONGBLOB: length in: %d, length out: %d", len(in), len(out))
		}
		if rows.Next() {
			db.Error("LONGBLOB: unexpexted row")
		}
	} else {
		db.Fatalf("LONGBLOB: no data")
	}

	db.mustExec("DROP TABLE IF EXISTS test")
}

func TestLoadData(t *testing.T) {
	runTests(t, "TestLoadData", _loadData)
}

func _loadData(db *DBTest) {
	verifyLoadDataResult := func() {
		rows, err := db.Query("SELECT * FROM test")
		if err != nil {
			db.Fatal(err.Error())
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
				db.Fatal(err.Error())
			}
			if i != id {
				db.Fatalf("%d != %d", i, id)
			}
			if values[i-1] != value {
				db.Fatalf("%s != %s", values[i-1], value)
			}
		}
		err = rows.Err()
		if err != nil {
			db.Fatal(err.Error())
		}

		if i != 4 {
			db.Fatalf("Rows count mismatch. Got %d, want 4", i)
		}
	}
	file, err := ioutil.TempFile("", "gotest")
	defer os.Remove(file.Name())
	if err != nil {
		db.Fatal(err)
	}
	file.WriteString("1\ta string\n2\ta string containing a \\t\n3\ta string containing a \\n\n4\ta string containing both \\t\\n\n")
	file.Close()

	db.mustExec("DROP TABLE IF EXISTS test")
	db.mustExec("CREATE TABLE test (id INT NOT NULL PRIMARY KEY, value TEXT NOT NULL) CHARACTER SET utf8 COLLATE utf8_unicode_ci")

	// Local File
	RegisterLocalFile(file.Name())
	db.mustExec(fmt.Sprintf("LOAD DATA LOCAL INFILE '%q' INTO TABLE test", file.Name()))
	verifyLoadDataResult()
	// negative test
	_, err = db.Exec("LOAD DATA LOCAL INFILE 'doesnotexist' INTO TABLE test")
	if err == nil {
		db.Fatal("Load non-existent file didn't fail")
	} else if err.Error() != "Local File 'doesnotexist' is not registered. Use the DSN parameter 'allowAllFiles=true' to allow all files" {
		db.Fatal(err.Error())
	}

	// Empty table
	db.mustExec("TRUNCATE TABLE test")

	// Reader
	RegisterReaderHandler("test", func() io.Reader {
		file, err = os.Open(file.Name())
		if err != nil {
			db.Fatal(err)
		}
		return file
	})
	db.mustExec("LOAD DATA LOCAL INFILE 'Reader::test' INTO TABLE test")
	verifyLoadDataResult()
	// negative test
	_, err = db.Exec("LOAD DATA LOCAL INFILE 'Reader::doesnotexist' INTO TABLE test")
	if err == nil {
		db.Fatal("Load non-existent Reader didn't fail")
	} else if err.Error() != "Reader 'doesnotexist' is not registered" {
		db.Fatal(err.Error())
	}

	db.mustExec("DROP TABLE IF EXISTS test")
}

// Special cases

func TestRowsClose(t *testing.T) {
	runTests(t, "TestRowsClose", _rowsClose)
}

func _rowsClose(db *DBTest) {
	rows, err := db.Query("SELECT 1")
	if err != nil {
		db.Fatal(err)
	}

	err = rows.Close()
	if err != nil {
		db.Fatal(err)
	}

	if rows.Next() {
		db.Fatal("Unexpected row after rows.Close()")
	}

	err = rows.Err()
	if err != nil {
		db.Fatal(err)
	}

}

// dangling statements
// http://code.google.com/p/go/issues/detail?id=3865
func TestCloseStmtBeforeRows(t *testing.T) {
	runTests(t, "TestCloseStmtBeforeRows", _closeStmtBeforeRows)
}

func _closeStmtBeforeRows(db *DBTest) {
	stmt, err := db.Prepare("SELECT 1")
	if err != nil {
		db.Fatal(err)
	}

	rows, err := stmt.Query()
	if err != nil {
		stmt.Close()
		db.Fatal(err)
	}
	defer rows.Close()

	err = stmt.Close()
	if err != nil {
		db.Fatal(err)
	}

	if !rows.Next() {
		db.Fatal("Getting row failed")
	} else {
		err = rows.Err()
		if err != nil {
			db.Fatal(err)
		}

		var out bool
		err = rows.Scan(&out)
		if err != nil {
			db.Fatalf("Error on rows.Scan(): %v", err)
		}
		if out != true {
			db.Errorf("true != %t", out)
		}
	}
}

// It is valid to have multiple Rows for the same Stmt
// http://code.google.com/p/go/issues/detail?id=3734
func TestStmtMultiRows(t *testing.T) {
	runTests(t, "TestStmtMultiRows", _stmtMultiRows)
}

func _stmtMultiRows(db *DBTest) {
	stmt, err := db.Prepare("SELECT 1 UNION SELECT 0")
	if err != nil {
		db.Fatal(err)
	}

	rows1, err := stmt.Query()
	if err != nil {
		stmt.Close()
		db.Fatal(err)
	}
	defer rows1.Close()

	rows2, err := stmt.Query()
	if err != nil {
		stmt.Close()
		db.Fatal(err)
	}
	defer rows2.Close()

	var out bool

	// 1
	if !rows1.Next() {
		db.Fatal("1st rows1.Next failed")
	} else {
		err = rows1.Err()
		if err != nil {
			db.Fatal(err)
		}

		err = rows1.Scan(&out)
		if err != nil {
			db.Fatalf("Error on rows.Scan(): %v", err)
		}
		if out != true {
			db.Errorf("true != %t", out)
		}
	}

	if !rows2.Next() {
		db.Fatal("1st rows2.Next failed")
	} else {
		err = rows2.Err()
		if err != nil {
			db.Fatal(err)
		}

		err = rows2.Scan(&out)
		if err != nil {
			db.Fatalf("Error on rows.Scan(): %v", err)
		}
		if out != true {
			db.Errorf("true != %t", out)
		}
	}

	// 2
	if !rows1.Next() {
		db.Fatal("2nd rows1.Next failed")
	} else {
		err = rows1.Err()
		if err != nil {
			db.Fatal(err)
		}

		err = rows1.Scan(&out)
		if err != nil {
			db.Fatalf("Error on rows.Scan(): %v", err)
		}
		if out != false {
			db.Errorf("false != %t", out)
		}

		if rows1.Next() {
			db.Fatal("Unexpected row on rows1")
		}
		err = rows1.Close()
		if err != nil {
			db.Fatal(err)
		}
	}

	if !rows2.Next() {
		db.Fatal("2nd rows2.Next failed")
	} else {
		err = rows2.Err()
		if err != nil {
			db.Fatal(err)
		}

		err = rows2.Scan(&out)
		if err != nil {
			db.Fatalf("Error on rows.Scan(): %v", err)
		}
		if out != false {
			db.Errorf("false != %t", out)
		}

		if rows2.Next() {
			db.Fatal("Unexpected row on rows2")
		}
		err = rows2.Close()
		if err != nil {
			db.Fatal(err)
		}
	}
}

func TestConcurrent(t *testing.T) {
	if os.Getenv("MYSQL_TEST_CONCURRENT") != "1" {
		t.Log("CONCURRENT env var not set. Skipping TestConcurrent")
		return
	}
	runTests(t, "TestConcurrent", _concurrent)
}

func _concurrent(db *DBTest) {
	var max int
	err := db.QueryRow("SELECT @@max_connections").Scan(&max)
	if err != nil {
		db.Fatalf("%v", err)
	}

	db.Logf("Testing up to %d concurrent connections \r\n", max)

	canStop := false

	c := make(chan struct{}, max)
	for i := 0; i < max; i++ {
		go func(id int) {
			tx, err := db.Begin()
			if err != nil {
				canStop = true
				if err.Error() == "Error 1040: Too many connections" {
					max--
					return
				} else {
					db.Fatalf("Error on Con %d: %s", id, err.Error())
				}
			}

			c <- struct{}{}

			for !canStop {
				_, err = tx.Exec("SELECT 1")
				if err != nil {
					canStop = true
					db.Fatalf("Error on Con %d: %s", id, err.Error())
				}
			}

			err = tx.Commit()
			if err != nil {
				canStop = true
				db.Fatalf("Error on Con %d: %s", id, err.Error())
			}
		}(i)
	}

	for i := 0; i < max; i++ {
		<-c
	}
	canStop = true

	db.Logf("Reached %d concurrent connections \r\n", max)
}

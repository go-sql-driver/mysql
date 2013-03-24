package mysql

import (
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
)

var (
	charset string
	dsn     string
	netAddr string
	run     bool
	once    sync.Once
)

// See https://github.com/go-sql-driver/mysql/wiki/Testing
func getEnv() bool {
	once.Do(func() {
		user := os.Getenv("MYSQL_TEST_USER")
		if user == "" {
			user = "root"
		}

		pass := os.Getenv("MYSQL_TEST_PASS")

		prot := os.Getenv("MYSQL_TEST_PROT")
		if prot == "" {
			prot = "tcp"
		}

		addr := os.Getenv("MYSQL_TEST_ADDR")
		if addr == "" {
			addr = "localhost:3306"
		}

		dbname := os.Getenv("MYSQL_TEST_DBNAME")
		if dbname == "" {
			dbname = "gotest"
		}

		charset = "charset=utf8"
		netAddr = fmt.Sprintf("%s(%s)", prot, addr)
		dsn = fmt.Sprintf("%s:%s@%s/%s?timeout=30s&"+charset, user, pass, netAddr, dbname)

		c, err := net.Dial(prot, addr)
		if err == nil {
			run = true
			c.Close()
		}
	})

	return run
}

func mustExec(t *testing.T, db *sql.DB, query string, args ...interface{}) (res sql.Result) {
	res, err := db.Exec(query, args...)
	if err != nil {
		if len(query) > 300 {
			query = "[query too large to print]"
		}
		t.Fatalf("Error on Exec %s: %v", query, err)
	}
	return
}

func mustQuery(t *testing.T, db *sql.DB, query string, args ...interface{}) (rows *sql.Rows) {
	rows, err := db.Query(query, args...)
	if err != nil {
		if len(query) > 300 {
			query = "[query too large to print]"
		}
		t.Fatalf("Error on Query %s: %v", query, err)
	}
	return
}

func mustSetCharset(t *testing.T, charsetParam, expected string) {
	db, err := sql.Open("mysql", strings.Replace(dsn, charset, charsetParam, 1))
	if err != nil {
		t.Fatalf("Error on Open: %v", err)
	}

	rows := mustQuery(t, db, ("SELECT @@character_set_connection"))
	if !rows.Next() {
		t.Fatalf("Error getting connection charset: %v", err)
	}

	var got string
	rows.Scan(&got)

	if got != expected {
		t.Fatalf("Expected connection charset %s but got %s", expected, got)
	}
}

func TestCharset(t *testing.T) {
	if !getEnv() {
		t.Logf("MySQL-Server not running on %s. Skipping TestCharset", netAddr)
		return
	}

	// non utf8 test
	mustSetCharset(t, "charset=ascii", "ascii")
}

func TestFailingCharset(t *testing.T) {
	db, err := sql.Open("mysql", strings.Replace(dsn, charset, "charset=none", 1))
	// run query to really establish connection...
	_, err = db.Exec("SELECT 1")
	if err == nil {
		db.Close()
		t.Fatalf("Connection must not succeed without a valid charset")
	}
}

func TestFallbackCharset(t *testing.T) {
	if !getEnv() {
		t.Logf("MySQL-Server not running on %s. Skipping TestFallbackCharset", netAddr)
		return
	}

	// when the first charset is invalid, use the second
	mustSetCharset(t, "charset=none,utf8", "utf8")

	// when the first charset is valid, use it
	charsets := []string{"ascii", "utf8"}
	for i := range charsets {
		expected := charsets[i]
		other := charsets[1-i]
		mustSetCharset(t, "charset="+expected+","+other, expected)
	}
}

func TestCRUD(t *testing.T) {
	if !getEnv() {
		t.Logf("MySQL-Server not running on %s. Skipping TestCRUD", netAddr)
		return
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}

	defer db.Close()

	mustExec(t, db, "DROP TABLE IF EXISTS test")

	// Create Table
	mustExec(t, db, "CREATE TABLE test (value BOOL)")

	// Test for unexpected data
	var out bool
	rows := mustQuery(t, db, ("SELECT * FROM test"))
	if rows.Next() {
		t.Error("unexpected data in empty table")
	}

	// Create Data
	res := mustExec(t, db, ("INSERT INTO test VALUES (1)"))
	count, err := res.RowsAffected()
	if err != nil {
		t.Fatalf("res.RowsAffected() returned error: %v", err)
	}
	if count != 1 {
		t.Fatalf("Expected 1 affected row, got %d", count)
	}

	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("res.LastInsertId() returned error: %v", err)
	}
	if id != 0 {
		t.Fatalf("Expected InsertID 0, got %d", id)
	}

	// Read
	rows = mustQuery(t, db, ("SELECT value FROM test"))
	if rows.Next() {
		rows.Scan(&out)
		if true != out {
			t.Errorf("true != %t", out)
		}

		if rows.Next() {
			t.Error("unexpected data")
		}
	} else {
		t.Error("no data")
	}

	// Update
	res = mustExec(t, db, "UPDATE test SET value = ? WHERE value = ?", false, true)
	count, err = res.RowsAffected()
	if err != nil {
		t.Fatalf("res.RowsAffected() returned error: %v", err)
	}
	if count != 1 {
		t.Fatalf("Expected 1 affected row, got %d", count)
	}

	// Check Update
	rows = mustQuery(t, db, ("SELECT value FROM test"))
	if rows.Next() {
		rows.Scan(&out)
		if false != out {
			t.Errorf("false != %t", out)
		}

		if rows.Next() {
			t.Error("unexpected data")
		}
	} else {
		t.Error("no data")
	}

	// Delete
	res = mustExec(t, db, "DELETE FROM test WHERE value = ?", false)
	count, err = res.RowsAffected()
	if err != nil {
		t.Fatalf("res.RowsAffected() returned error: %v", err)
	}
	if count != 1 {
		t.Fatalf("Expected 1 affected row, got %d", count)
	}

	// Check for unexpected rows
	res = mustExec(t, db, "DELETE FROM test")
	count, err = res.RowsAffected()
	if err != nil {
		t.Fatalf("res.RowsAffected() returned error: %v", err)
	}
	if count != 0 {
		t.Fatalf("Expected 0 affected row, got %d", count)
	}
}

func TestInt(t *testing.T) {
	if !getEnv() {
		t.Logf("MySQL-Server not running on %s. Skipping TestInt", netAddr)
		return
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}

	defer db.Close()

	mustExec(t, db, "DROP TABLE IF EXISTS test")

	types := [5]string{"TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT"}
	in := int64(42)
	var out int64
	var rows *sql.Rows

	// SIGNED
	for _, v := range types {
		mustExec(t, db, "CREATE TABLE test (value "+v+")")

		mustExec(t, db, ("INSERT INTO test VALUES (?)"), in)

		rows = mustQuery(t, db, ("SELECT value FROM test"))
		if rows.Next() {
			rows.Scan(&out)
			if in != out {
				t.Errorf("%s: %d != %d", v, in, out)
			}
		} else {
			t.Errorf("%s: no data", v)
		}

		mustExec(t, db, "DROP TABLE IF EXISTS test")
	}

	// UNSIGNED ZEROFILL
	for _, v := range types {
		mustExec(t, db, "CREATE TABLE test (value "+v+" ZEROFILL)")

		mustExec(t, db, ("INSERT INTO test VALUES (?)"), in)

		rows = mustQuery(t, db, ("SELECT value FROM test"))
		if rows.Next() {
			rows.Scan(&out)
			if in != out {
				t.Errorf("%s ZEROFILL: %d != %d", v, in, out)
			}
		} else {
			t.Errorf("%s ZEROFILL: no data", v)
		}

		mustExec(t, db, "DROP TABLE IF EXISTS test")
	}
}

func TestFloat(t *testing.T) {
	if !getEnv() {
		t.Logf("MySQL-Server not running on %s. Skipping TestFloat", netAddr)
		return
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}

	defer db.Close()

	mustExec(t, db, "DROP TABLE IF EXISTS test")

	types := [2]string{"FLOAT", "DOUBLE"}
	in := float32(42.23)
	var out float32
	var rows *sql.Rows

	for _, v := range types {
		mustExec(t, db, "CREATE TABLE test (value "+v+")")

		mustExec(t, db, ("INSERT INTO test VALUES (?)"), in)

		rows = mustQuery(t, db, ("SELECT value FROM test"))
		if rows.Next() {
			rows.Scan(&out)
			if in != out {
				t.Errorf("%s: %g != %g", v, in, out)
			}
		} else {
			t.Errorf("%s: no data", v)
		}

		mustExec(t, db, "DROP TABLE IF EXISTS test")
	}
}

func TestString(t *testing.T) {
	if !getEnv() {
		t.Logf("MySQL-Server not running on %s. Skipping TestString", netAddr)
		return
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}

	defer db.Close()

	mustExec(t, db, "DROP TABLE IF EXISTS test")

	types := [6]string{"CHAR(255)", "VARCHAR(255)", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT"}
	in := "κόσμε üöäßñóùéàâÿœ'îë Árvíztűrő いろはにほへとちりぬるを イロハニホヘト דג סקרן чащах  น่าฟังเอย"
	var out string
	var rows *sql.Rows

	for _, v := range types {
		mustExec(t, db, "CREATE TABLE test (value "+v+") CHARACTER SET utf8 COLLATE utf8_unicode_ci")

		mustExec(t, db, ("INSERT INTO test VALUES (?)"), in)

		rows = mustQuery(t, db, ("SELECT value FROM test"))
		if rows.Next() {
			rows.Scan(&out)
			if in != out {
				t.Errorf("%s: %s != %s", v, in, out)
			}
		} else {
			t.Errorf("%s: no data", v)
		}

		mustExec(t, db, "DROP TABLE IF EXISTS test")
	}

	// BLOB
	mustExec(t, db, "CREATE TABLE test (id int, value BLOB) CHARACTER SET utf8 COLLATE utf8_unicode_ci")

	id := 2
	in = "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, " +
		"sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, " +
		"sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. " +
		"Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. " +
		"Lorem ipsum dolor sit amet, consetetur sadipscing elitr, " +
		"sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, " +
		"sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. " +
		"Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet."
	mustExec(t, db, ("INSERT INTO test VALUES (?, ?)"), id, in)

	err = db.QueryRow("SELECT value FROM test WHERE id = ?", id).Scan(&out)
	if err != nil {
		t.Fatalf("Error on BLOB-Query: %v", err)
	} else if out != in {
		t.Errorf("BLOB: %s != %s", in, out)
	}

	return
}

func TestDateTime(t *testing.T) {
	if !getEnv() {
		t.Logf("MySQL-Server not running on %s. Skipping TestString", netAddr)
		return
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}

	defer db.Close()

	mustExec(t, db, "DROP TABLE IF EXISTS test")

	types := [...]string{"DATE", "DATETIME"}
	in := [...]string{"2012-06-14", "2011-11-20 21:27:37"}
	var out string
	var rows *sql.Rows

	for i, v := range types {
		mustExec(t, db, "CREATE TABLE test (value "+v+") CHARACTER SET utf8 COLLATE utf8_unicode_ci")

		mustExec(t, db, ("INSERT INTO test VALUES (?)"), in[i])

		rows = mustQuery(t, db, ("SELECT value FROM test"))
		if rows.Next() {
			rows.Scan(&out)
			if in[i] != out {
				t.Errorf("%s: %s != %s", v, in[i], out)
			}
		} else {
			t.Errorf("%s: no data", v)
		}

		mustExec(t, db, "DROP TABLE IF EXISTS test")
	}
}

func TestNULL(t *testing.T) {
	if !getEnv() {
		t.Logf("MySQL-Server not running on %s. Skipping TestNULL", netAddr)
		return
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}

	defer db.Close()

	nullStmt, err := db.Prepare("SELECT NULL")
	if err != nil {
		t.Fatal(err)
	}
	defer nullStmt.Close()

	nonNullStmt, err := db.Prepare("SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	defer nonNullStmt.Close()

	// NullBool
	var nb sql.NullBool
	// Invalid
	err = nullStmt.QueryRow().Scan(&nb)
	if err != nil {
		t.Fatal(err)
	}
	if nb.Valid {
		t.Error("Valid NullBool which should be invalid")
	}
	// Valid
	err = nonNullStmt.QueryRow().Scan(&nb)
	if err != nil {
		t.Fatal(err)
	}
	if !nb.Valid {
		t.Error("Invalid NullBool which should be valid")
	} else if nb.Bool != true {
		t.Errorf("Unexpected NullBool value: %t (should be true)", nb.Bool)
	}

	// NullFloat64
	var nf sql.NullFloat64
	// Invalid
	err = nullStmt.QueryRow().Scan(&nf)
	if err != nil {
		t.Fatal(err)
	}
	if nf.Valid {
		t.Error("Valid NullFloat64 which should be invalid")
	}
	// Valid
	err = nonNullStmt.QueryRow().Scan(&nf)
	if err != nil {
		t.Fatal(err)
	}
	if !nf.Valid {
		t.Error("Invalid NullFloat64 which should be valid")
	} else if nf.Float64 != float64(1) {
		t.Errorf("Unexpected NullFloat64 value: %f (should be 1.0)", nf.Float64)
	}

	// NullInt64
	var ni sql.NullInt64
	// Invalid
	err = nullStmt.QueryRow().Scan(&ni)
	if err != nil {
		t.Fatal(err)
	}
	if ni.Valid {
		t.Error("Valid NullInt64 which should be invalid")
	}
	// Valid
	err = nonNullStmt.QueryRow().Scan(&ni)
	if err != nil {
		t.Fatal(err)
	}
	if !ni.Valid {
		t.Error("Invalid NullInt64 which should be valid")
	} else if ni.Int64 != int64(1) {
		t.Errorf("Unexpected NullInt64 value: %d (should be 1)", ni.Int64)
	}

	// NullString
	var ns sql.NullString
	// Invalid
	err = nullStmt.QueryRow().Scan(&ns)
	if err != nil {
		t.Fatal(err)
	}
	if ns.Valid {
		t.Error("Valid NullString which should be invalid")
	}
	// Valid
	err = nonNullStmt.QueryRow().Scan(&ns)
	if err != nil {
		t.Fatal(err)
	}
	if !ns.Valid {
		t.Error("Invalid NullString which should be valid")
	} else if ns.String != `1` {
		t.Error("Unexpected NullString value:" + ns.String + " (should be `1`)")
	}

	// Insert NULL
	mustExec(t, db, "CREATE TABLE test (dummmy1 int, value int, dummy2 int)")

	mustExec(t, db, ("INSERT INTO test VALUES (?, ?, ?)"), 1, nil, 2)

	var out interface{}
	rows := mustQuery(t, db, ("SELECT * FROM test"))
	if rows.Next() {
		rows.Scan(&out)
		if out != nil {
			t.Errorf("%v != nil", out)
		}
	} else {
		t.Error("no data")
	}

	mustExec(t, db, "DROP TABLE IF EXISTS test")
}

func TestLongData(t *testing.T) {
	if !getEnv() {
		t.Logf("MySQL-Server not running on %s. Skipping TestLongData", netAddr)
		return
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}
	defer db.Close()

	var maxAllowedPacketSize int
	err = db.QueryRow("select @@max_allowed_packet").Scan(&maxAllowedPacketSize)
	if err != nil {
		t.Fatal(err)
	}
	maxAllowedPacketSize--

	// don't get too ambitious
	if maxAllowedPacketSize > 1<<25 {
		maxAllowedPacketSize = 1 << 25
	}

	mustExec(t, db, "DROP TABLE IF EXISTS test")
	mustExec(t, db, "CREATE TABLE test (value LONGBLOB) CHARACTER SET utf8 COLLATE utf8_unicode_ci")

	in := strings.Repeat(`0`, maxAllowedPacketSize+1)
	var out string
	var rows *sql.Rows

	// Long text data
	const nonDataQueryLen = 28 // length query w/o value
	inS := in[:maxAllowedPacketSize-nonDataQueryLen]
	mustExec(t, db, "INSERT INTO test VALUES('"+inS+"')")
	rows = mustQuery(t, db, "SELECT value FROM test")
	if rows.Next() {
		rows.Scan(&out)
		if inS != out {
			t.Fatalf("LONGBLOB: length in: %d, length out: %d", len(inS), len(out))
		}
		if rows.Next() {
			t.Error("LONGBLOB: unexpexted row")
		}
	} else {
		t.Fatalf("LONGBLOB: no data")
	}

	// Empty table
	mustExec(t, db, "TRUNCATE TABLE test")

	// Long binary data
	mustExec(t, db, "INSERT INTO test VALUES(?)", in)
	rows = mustQuery(t, db, "SELECT value FROM test WHERE 1=?", 1)
	if rows.Next() {
		rows.Scan(&out)
		if in != out {
			t.Fatalf("LONGBLOB: length in: %d, length out: %d", len(in), len(out))
		}
		if rows.Next() {
			t.Error("LONGBLOB: unexpexted row")
		}
	} else {
		t.Fatalf("LONGBLOB: no data")
	}

	mustExec(t, db, "DROP TABLE IF EXISTS test")
}

func verifyLoadDataResult(t *testing.T, db *sql.DB) {
	rows, err := db.Query("SELECT * FROM test")
	if err != nil {
		t.Fatal(err.Error())
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
			t.Fatal(err.Error())
		}
		if i != id {
			t.Fatalf("%d != %d", i, id)
		}
		if values[i-1] != value {
			t.Fatalf("%s != %s", values[i-1], value)
		}
	}
	err = rows.Err()
	if err != nil {
		t.Fatal(err.Error())
	}

	if i != 4 {
		t.Fatalf("Rows count mismatch. Got %d, want 4", i)
	}
}

func TestLoadData(t *testing.T) {
	if !getEnv() {
		t.Logf("MySQL-Server not running on %s. Skipping TestLoadData", netAddr)
		return
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}
	defer db.Close()

	file, err := ioutil.TempFile("", "gotest")
	defer os.Remove(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	file.WriteString("1\ta string\n2\ta string containing a \\t\n3\ta string containing a \\n\n4\ta string containing both \\t\\n\n")
	file.Close()

	mustExec(t, db, "DROP TABLE IF EXISTS test")
	mustExec(t, db, "CREATE TABLE test (id INT NOT NULL PRIMARY KEY, value TEXT NOT NULL) CHARACTER SET utf8 COLLATE utf8_unicode_ci")

	// Local File
	RegisterLocalFile(file.Name())
	mustExec(t, db, fmt.Sprintf("LOAD DATA LOCAL INFILE '%q' INTO TABLE test", file.Name()))
	verifyLoadDataResult(t, db)
	// negative test
	_, err = db.Exec("LOAD DATA LOCAL INFILE 'doesnotexist' INTO TABLE test")
	if err == nil {
		t.Fatal("Load non-existent file didn't fail")
	} else if err.Error() != "Local File 'doesnotexist' is not registered. Use the DSN parameter 'allowAllFiles=true' to allow all files" {
		t.Fatal(err.Error())
	}

	// Empty table
	mustExec(t, db, "TRUNCATE TABLE test")

	// Reader
	RegisterReaderHandler("test", func() io.Reader {
		file, err = os.Open(file.Name())
		if err != nil {
			t.Fatal(err)
		}
		return file
	})
	mustExec(t, db, "LOAD DATA LOCAL INFILE 'Reader::test' INTO TABLE test")
	verifyLoadDataResult(t, db)
	// negative test
	_, err = db.Exec("LOAD DATA LOCAL INFILE 'Reader::doesnotexist' INTO TABLE test")
	if err == nil {
		t.Fatal("Load non-existent Reader didn't fail")
	} else if err.Error() != "Reader 'doesnotexist' is not registered" {
		t.Fatal(err.Error())
	}

	mustExec(t, db, "DROP TABLE IF EXISTS test")
}

// Special cases

func TestRowsClose(t *testing.T) {
	if !getEnv() {
		t.Logf("MySQL-Server not running on %s. Skipping TestRowsClose", netAddr)
		return
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}

	defer db.Close()

	rows, err := db.Query("SELECT 1")
	if err != nil {
		t.Fatal(err)
	}

	err = rows.Close()
	if err != nil {
		t.Fatal(err)
	}

	if rows.Next() {
		t.Fatal("Unexpected row after rows.Close()")
	}

	err = rows.Err()
	if err != nil {
		t.Fatal(err)
	}
}

// dangling statements
// http://code.google.com/p/go/issues/detail?id=3865
func TestCloseStmtBeforeRows(t *testing.T) {
	if !getEnv() {
		t.Logf("MySQL-Server not running on %s. Skipping TestCloseStmtBeforeRows", netAddr)
		return
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}

	defer db.Close()

	stmt, err := db.Prepare("SELECT 1")
	if err != nil {
		t.Fatal(err)
	}

	rows, err := stmt.Query()
	if err != nil {
		stmt.Close()
		t.Fatal(err)
	}
	defer rows.Close()

	err = stmt.Close()
	if err != nil {
		t.Fatal(err)
	}

	if !rows.Next() {
		t.Fatal("Getting row failed")
	} else {
		err = rows.Err()
		if err != nil {
			t.Fatal(err)
		}

		var out bool
		err = rows.Scan(&out)
		if err != nil {
			t.Fatalf("Error on rows.Scan(): %v", err)
		}
		if out != true {
			t.Errorf("true != %t", out)
		}
	}
}

// It is valid to have multiple Rows for the same Stmt
// http://code.google.com/p/go/issues/detail?id=3734
func TestStmtMultiRows(t *testing.T) {
	if !getEnv() {
		t.Logf("MySQL-Server not running on %s. Skipping TestStmtMultiRows", netAddr)
		return
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}

	defer db.Close()

	stmt, err := db.Prepare("SELECT 1 UNION SELECT 0")
	if err != nil {
		t.Fatal(err)
	}

	rows1, err := stmt.Query()
	if err != nil {
		stmt.Close()
		t.Fatal(err)
	}
	defer rows1.Close()

	rows2, err := stmt.Query()
	if err != nil {
		stmt.Close()
		t.Fatal(err)
	}
	defer rows2.Close()

	var out bool

	// 1
	if !rows1.Next() {
		t.Fatal("1st rows1.Next failed")
	} else {
		err = rows1.Err()
		if err != nil {
			t.Fatal(err)
		}

		err = rows1.Scan(&out)
		if err != nil {
			t.Fatalf("Error on rows.Scan(): %v", err)
		}
		if out != true {
			t.Errorf("true != %t", out)
		}
	}

	if !rows2.Next() {
		t.Fatal("1st rows2.Next failed")
	} else {
		err = rows2.Err()
		if err != nil {
			t.Fatal(err)
		}

		err = rows2.Scan(&out)
		if err != nil {
			t.Fatalf("Error on rows.Scan(): %v", err)
		}
		if out != true {
			t.Errorf("true != %t", out)
		}
	}

	// 2
	if !rows1.Next() {
		t.Fatal("2nd rows1.Next failed")
	} else {
		err = rows1.Err()
		if err != nil {
			t.Fatal(err)
		}

		err = rows1.Scan(&out)
		if err != nil {
			t.Fatalf("Error on rows.Scan(): %v", err)
		}
		if out != false {
			t.Errorf("false != %t", out)
		}

		if rows1.Next() {
			t.Fatal("Unexpected row on rows1")
		}
		err = rows1.Close()
		if err != nil {
			t.Fatal(err)
		}
	}

	if !rows2.Next() {
		t.Fatal("2nd rows2.Next failed")
	} else {
		err = rows2.Err()
		if err != nil {
			t.Fatal(err)
		}

		err = rows2.Scan(&out)
		if err != nil {
			t.Fatalf("Error on rows.Scan(): %v", err)
		}
		if out != false {
			t.Errorf("false != %t", out)
		}

		if rows2.Next() {
			t.Fatal("Unexpected row on rows2")
		}
		err = rows2.Close()
		if err != nil {
			t.Fatal(err)
		}
	}

}

func TestConcurrent(t *testing.T) {
	if os.Getenv("MYSQL_TEST_CONCURRENT") != "1" {
		t.Log("CONCURRENT env var not set. Skipping TestConcurrent")
		return
	}
	if !getEnv() {
		t.Logf("MySQL-Server not running on %s. Skipping TestConcurrent", netAddr)
		return
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}

	defer db.Close()

	var max int
	err = db.QueryRow("SELECT @@max_connections").Scan(&max)
	if err != nil {
		t.Fatalf("%v", err)
	}

	t.Logf("Testing up to %d concurrent connections \r\n", max)

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
					t.Fatalf("Error on Con %d: %s", id, err.Error())
				}
			}

			c <- struct{}{}

			for !canStop {
				_, err = tx.Exec("SELECT 1")
				if err != nil {
					canStop = true
					t.Fatalf("Error on Con %d: %s", id, err.Error())
				}
			}

			err = tx.Commit()
			if err != nil {
				canStop = true
				t.Fatalf("Error on Con %d: %s", id, err.Error())
			}
		}(i)
	}

	for i := 0; i < max; i++ {
		<-c
	}
	canStop = true

	t.Logf("Reached %d concurrent connections \r\n", max)
}

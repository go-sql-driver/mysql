package mysql

import (
	"database/sql"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
)

var (
	dsn     string
	netAddr string
	run     bool
	once    sync.Once
)

// See https://github.com/Go-SQL-Driver/MySQL/wiki/Testing
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

		netAddr = fmt.Sprintf("%s(%s)", prot, addr)
		dsn = fmt.Sprintf("%s:%s@%s/%s?charset=utf8", user, pass, netAddr, dbname)

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
		t.Fatalf("Error on Exec %q: %v", query, err)
	}
	return
}

func mustQuery(t *testing.T, db *sql.DB, query string, args ...interface{}) (rows *sql.Rows) {
	rows, err := db.Query(query, args...)
	if err != nil {
		t.Fatalf("Error on Query %q: %v", query, err)
	}
	return
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
			t.Errorf("true != %d", out)
		}

		if rows.Next() {
			t.Error("unexpected data")
		}
	} else {
		t.Error("no data")
	}

	// Update
	mustExec(t, db, "UPDATE test SET value = ? WHERE value = ?", false, true)
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
			t.Errorf("false != %d", out)
		}

		if rows.Next() {
			t.Error("unexpected data")
		}
	} else {
		t.Error("no data")
	}

	// Delete
	mustExec(t, db, "DELETE FROM test WHERE value = ?", false)
	count, err = res.RowsAffected()
	if err != nil {
		t.Fatalf("res.RowsAffected() returned error: %v", err)
	}
	if count != 1 {
		t.Fatalf("Expected 1 affected row, got %d", count)
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
	in := float64(42.23)
	var out float64
	var rows *sql.Rows

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
				t.Errorf("%s: %d != %d", v, in, out)
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
			t.Errorf("true != %d", out)
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
			t.Errorf("true != %d", out)
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
			t.Errorf("true != %d", out)
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
			t.Errorf("false != %d", out)
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
			t.Errorf("false != %d", out)
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

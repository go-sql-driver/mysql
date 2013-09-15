package mysql

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

type TB testing.B

func (tb *TB) check(err error) {
	if err != nil {
		tb.Fatal(err)
	}
}

func (tb *TB) checkDB(db *sql.DB, err error) *sql.DB {
	tb.check(err)
	return db
}

func (tb *TB) checkRes(res sql.Result, err error) sql.Result {
	tb.check(err)
	return res
}

func (tb *TB) checkRow(row *sql.Row, err error) *sql.Row {
	tb.check(err)
	return row
}

func (tb *TB) checkRows(rows *sql.Rows, err error) *sql.Rows {
	tb.check(err)
	return rows
}

func (tb *TB) checkStmt(stmt *sql.Stmt, err error) *sql.Stmt {
	tb.check(err)
	return stmt
}

func initDB(b *testing.B, queries ...string) *sql.DB {
	tb := (*TB)(b)
	db := tb.checkDB(sql.Open("mysql", dsn))
	//db.SetMaxIdleConns(concurrencyLevel)
	for _, query := range queries {
		db.Exec(query)
	}
	return db
}

// by Brad Fitzpatrick
const concurrencyLevel = 10

func BenchmarkQuery(b *testing.B) {
	tb := (*TB)(b)
	b.StopTimer()
	reportAllocs(b)
	db := initDB(b,
		"DROP TABLE IF EXISTS foo",
		"CREATE TABLE foo (id INT PRIMARY KEY, val CHAR(50))",
		`INSERT INTO foo VALUES (1, "one")`,
		`INSERT INTO foo VALUES (2, "two")`,
	)
	defer db.Close()

	stmt := tb.checkStmt(db.Prepare("SELECT val FROM foo WHERE id=?"))
	defer stmt.Close()
	b.StartTimer()

	remain := int64(b.N)
	var wg sync.WaitGroup
	wg.Add(concurrencyLevel)
	defer wg.Wait()
	for i := 0; i < concurrencyLevel; i++ {
		go func() {
			defer wg.Done()
			for {
				if atomic.AddInt64(&remain, -1) < 0 {
					return
				}
				var got string
				tb.check(stmt.QueryRow(1).Scan(&got))
				if got != "one" {
					b.Errorf("query = %q; want one", got)
					return
				}
			}
		}()
	}
}

// data, but no db writes
var (
	roundtripSample []byte
	roundtripOnce   sync.Once
)

func initRoundtripBenchmarks() ([]byte, int, int) {
	roundtripOnce.Do(func() {
		roundtripSample = []byte(strings.Repeat("0123456789abcdef", 1024*1024))
	})
	return roundtripSample, 16, len(roundtripSample)
}

// Workaround to get Go 1.0 compatibility for Travis-CI
type AllocReporter interface {
	ReportAllocs()
}

func reportAllocs(b interface{}) {
	if reporter, ok := b.(AllocReporter); ok {
		reporter.ReportAllocs()
	}
}

func BenchmarkRoundtripTxt(b *testing.B) {
	b.StopTimer()
	sample, min, max := initRoundtripBenchmarks()
	reportAllocs(b)
	tb := (*TB)(b)
	db := tb.checkDB(sql.Open("mysql", dsn))
	defer db.Close()
	b.StartTimer()
	var result string
	for i := 0; i < b.N; i++ {
		length := min + i
		if length > max {
			length = max
		}
		test := string(sample[0:length])
		rows := tb.checkRows(db.Query("SELECT \"" + test + "\""))
		if !rows.Next() {
			rows.Close()
			b.Fatalf("crashed")
		}
		err := rows.Scan(&result)
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

func BenchmarkRoundtripBin(b *testing.B) {
	b.StopTimer()
	sample, min, max := initRoundtripBenchmarks()
	reportAllocs(b)
	tb := (*TB)(b)
	db := tb.checkDB(sql.Open("mysql", dsn))
	defer db.Close()
	stmt := tb.checkStmt(db.Prepare("SELECT ?"))
	defer stmt.Close()
	b.StartTimer()
	var result string
	for i := 0; i < b.N; i++ {
		length := min + i
		if length > max {
			length = max
		}
		test := string(sample[0:length])
		rows := tb.checkRows(stmt.Query(test))
		if !rows.Next() {
			rows.Close()
			b.Fatalf("crashed")
		}
		err := rows.Scan(&result)
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

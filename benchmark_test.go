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
	"database/sql"
	"database/sql/driver"
	"fmt"
	"math"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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

func (tb *TB) checkRows(rows *sql.Rows, err error) *sql.Rows {
	tb.check(err)
	return rows
}

func (tb *TB) checkStmt(stmt *sql.Stmt, err error) *sql.Stmt {
	tb.check(err)
	return stmt
}

func initDB(b *testing.B, compress bool, queries ...string) *sql.DB {
	tb := (*TB)(b)
	comprStr := ""
	if compress {
		comprStr = "&compress=1"
	}
	db := tb.checkDB(sql.Open(driverNameTest, dsn+comprStr))
	for _, query := range queries {
		if _, err := db.Exec(query); err != nil {
			b.Fatalf("error on %q: %v", query, err)
		}
	}
	return db
}

const concurrencyLevel = 10

func BenchmarkQuery(b *testing.B) {
	benchmarkQuery(b, false)
}

func BenchmarkQueryCompressed(b *testing.B) {
	benchmarkQuery(b, true)
}

func benchmarkQuery(b *testing.B, compr bool) {
	tb := (*TB)(b)
	b.ReportAllocs()
	db := initDB(b, compr,
		"DROP TABLE IF EXISTS foo",
		"CREATE TABLE foo (id INT PRIMARY KEY, val CHAR(50))",
		`INSERT INTO foo VALUES (1, "one")`,
		`INSERT INTO foo VALUES (2, "two")`,
	)
	db.SetMaxIdleConns(concurrencyLevel)
	defer db.Close()

	stmt := tb.checkStmt(db.Prepare("SELECT val FROM foo WHERE id=?"))
	defer stmt.Close()

	remain := int64(b.N)
	var wg sync.WaitGroup
	wg.Add(concurrencyLevel)
	defer wg.Wait()
	b.StartTimer()

	for i := 0; i < concurrencyLevel; i++ {
		go func() {
			for {
				if atomic.AddInt64(&remain, -1) < 0 {
					wg.Done()
					return
				}

				var got string
				tb.check(stmt.QueryRow(1).Scan(&got))
				if got != "one" {
					b.Errorf("query = %q; want one", got)
					wg.Done()
					return
				}
			}
		}()
	}
}

func BenchmarkExec(b *testing.B) {
	tb := (*TB)(b)
	db := tb.checkDB(sql.Open(driverNameTest, dsn))
	db.SetMaxIdleConns(concurrencyLevel)
	defer db.Close()

	stmt := tb.checkStmt(db.Prepare("DO 1"))
	defer stmt.Close()

	remain := int64(b.N)
	var wg sync.WaitGroup
	wg.Add(concurrencyLevel)
	defer wg.Wait()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < concurrencyLevel; i++ {
		go func() {
			for {
				if atomic.AddInt64(&remain, -1) < 0 {
					wg.Done()
					return
				}

				if _, err := stmt.Exec(); err != nil {
					b.Logf("stmt.Exec failed: %v", err)
					b.Fail()
				}
			}
		}()
	}
}

// data, but no db writes
var roundtripSample []byte

func initRoundtripBenchmarks() ([]byte, int, int) {
	if roundtripSample == nil {
		roundtripSample = []byte(strings.Repeat("0123456789abcdef", 1024*1024))
	}
	return roundtripSample, 16, len(roundtripSample)
}

func BenchmarkRoundtripTxt(b *testing.B) {
	sample, min, max := initRoundtripBenchmarks()
	sampleString := string(sample)
	tb := (*TB)(b)
	db := tb.checkDB(sql.Open(driverNameTest, dsn))
	defer db.Close()

	b.ReportAllocs()
	b.ResetTimer()

	var result string
	for i := 0; i < b.N; i++ {
		length := min + i
		if length > max {
			length = max
		}
		test := sampleString[0:length]
		rows := tb.checkRows(db.Query(`SELECT "` + test + `"`))
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
	sample, min, max := initRoundtripBenchmarks()
	tb := (*TB)(b)
	db := tb.checkDB(sql.Open(driverNameTest, dsn))
	defer db.Close()
	stmt := tb.checkStmt(db.Prepare("SELECT ?"))
	defer stmt.Close()

	b.ReportAllocs()
	b.ResetTimer()
	var result sql.RawBytes
	for i := 0; i < b.N; i++ {
		length := min + i
		if length > max {
			length = max
		}
		test := sample[0:length]
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
		if !bytes.Equal(result, test) {
			rows.Close()
			b.Errorf("mismatch")
		}
		rows.Close()
	}
}

func BenchmarkInterpolation(b *testing.B) {
	mc := &mysqlConn{
		cfg: &Config{
			InterpolateParams: true,
			Loc:               time.UTC,
		},
		maxAllowedPacket: maxPacketSize,
		maxWriteSize:     maxPacketSize - 1,
		buf:              newBuffer(),
	}

	args := []driver.Value{
		int64(42424242),
		float64(math.Pi),
		false,
		time.Unix(1423411542, 807015000),
		[]byte("bytes containing special chars ' \" \a \x00"),
		"string containing special chars ' \" \a \x00",
	}
	q := "SELECT ?, ?, ?, ?, ?, ?"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := mc.interpolateParams(q, args)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkQueryContext(b *testing.B, db *sql.DB, p int) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db.SetMaxIdleConns(p * runtime.GOMAXPROCS(0))

	tb := (*TB)(b)
	stmt := tb.checkStmt(db.PrepareContext(ctx, "SELECT val FROM foo WHERE id=?"))
	defer stmt.Close()

	b.SetParallelism(p)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var got string
		for pb.Next() {
			tb.check(stmt.QueryRow(1).Scan(&got))
			if got != "one" {
				b.Fatalf("query = %q; want one", got)
			}
		}
	})
}

func BenchmarkQueryContext(b *testing.B) {
	db := initDB(b, false,
		"DROP TABLE IF EXISTS foo",
		"CREATE TABLE foo (id INT PRIMARY KEY, val CHAR(50))",
		`INSERT INTO foo VALUES (1, "one")`,
		`INSERT INTO foo VALUES (2, "two")`,
	)
	defer db.Close()
	for _, p := range []int{1, 2, 3, 4} {
		b.Run(fmt.Sprintf("%d", p), func(b *testing.B) {
			benchmarkQueryContext(b, db, p)
		})
	}
}

func benchmarkExecContext(b *testing.B, db *sql.DB, p int) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db.SetMaxIdleConns(p * runtime.GOMAXPROCS(0))

	tb := (*TB)(b)
	stmt := tb.checkStmt(db.PrepareContext(ctx, "DO 1"))
	defer stmt.Close()

	b.SetParallelism(p)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := stmt.ExecContext(ctx); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkExecContext(b *testing.B) {
	db := initDB(b, false,
		"DROP TABLE IF EXISTS foo",
		"CREATE TABLE foo (id INT PRIMARY KEY, val CHAR(50))",
		`INSERT INTO foo VALUES (1, "one")`,
		`INSERT INTO foo VALUES (2, "two")`,
	)
	defer db.Close()
	for _, p := range []int{1, 2, 3, 4} {
		b.Run(fmt.Sprintf("%d", p), func(b *testing.B) {
			benchmarkExecContext(b, db, p)
		})
	}
}

// BenchmarkQueryRawBytes benchmarks fetching 100 blobs using sql.RawBytes.
// "size=" means size of each blobs.
func BenchmarkQueryRawBytes(b *testing.B) {
	var sizes []int = []int{100, 1000, 2000, 4000, 8000, 12000, 16000, 32000, 64000, 256000}
	db := initDB(b, false,
		"DROP TABLE IF EXISTS bench_rawbytes",
		"CREATE TABLE bench_rawbytes (id INT PRIMARY KEY, val LONGBLOB)",
	)
	defer db.Close()

	blob := make([]byte, sizes[len(sizes)-1])
	for i := range blob {
		blob[i] = 42
	}
	for i := 0; i < 100; i++ {
		_, err := db.Exec("INSERT INTO bench_rawbytes VALUES (?, ?)", i, blob)
		if err != nil {
			b.Fatal(err)
		}
	}

	for _, s := range sizes {
		b.Run(fmt.Sprintf("size=%v", s), func(b *testing.B) {
			db.SetMaxIdleConns(0)
			db.SetMaxIdleConns(1)
			b.ReportAllocs()
			b.ResetTimer()

			for j := 0; j < b.N; j++ {
				rows, err := db.Query("SELECT LEFT(val, ?) as v FROM bench_rawbytes", s)
				if err != nil {
					b.Fatal(err)
				}
				nrows := 0
				for rows.Next() {
					var buf sql.RawBytes
					err := rows.Scan(&buf)
					if err != nil {
						b.Fatal(err)
					}
					if len(buf) != s {
						b.Fatalf("size mismatch: expected %v, got %v", s, len(buf))
					}
					nrows++
				}
				rows.Close()
				if nrows != 100 {
					b.Fatalf("numbers of rows mismatch: expected %v, got %v", 100, nrows)
				}
			}
		})
	}
}

func benchmark10kRows(b *testing.B, compress bool) {
	// Setup -- prepare 10000 rows.
	db := initDB(b, compress,
		"DROP TABLE IF EXISTS foo",
		"CREATE TABLE foo (id INT PRIMARY KEY, val TEXT)")
	defer db.Close()

	sval := strings.Repeat("x", 50)
	stmt, err := db.Prepare(`INSERT INTO foo (id, val) VALUES (?, ?)` + strings.Repeat(",(?,?)", 99))
	if err != nil {
		b.Errorf("failed to prepare query: %v", err)
		return
	}

	args := make([]any, 200)
	for i := 1; i < 200; i += 2 {
		args[i] = sval
	}
	for i := 0; i < 10000; i += 100 {
		for j := 0; j < 100; j++ {
			args[j*2] = i + j
		}
		_, err := stmt.Exec(args...)
		if err != nil {
			b.Error(err)
			return
		}
	}
	stmt.Close()

	// benchmark function called several times with different b.N.
	// it means heavy setup is called multiple times.
	// Use b.Run() to run expensive setup only once.
	// Go 1.24 introduced b.Loop() for this purpose. But we keep this
	// benchmark compatible with Go 1.20.
	b.Run("query", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			rows, err := db.Query(`SELECT id, val FROM foo`)
			if err != nil {
				b.Errorf("failed to select: %v", err)
				return
			}
			// rows.Scan() escapes arguments. So these variables must be defined
			// before loop.
			var i int
			var s sql.RawBytes
			for rows.Next() {
				if err := rows.Scan(&i, &s); err != nil {
					b.Errorf("failed to scan: %v", err)
					rows.Close()
					return
				}
			}
			if err = rows.Err(); err != nil {
				b.Errorf("failed to read rows: %v", err)
			}
			rows.Close()
		}
	})
}

// BenchmarkReceive10kRows measures performance of receiving large number of rows.
func BenchmarkReceive10kRows(b *testing.B) {
	benchmark10kRows(b, false)
}

func BenchmarkReceive10kRowsCompressed(b *testing.B) {
	benchmark10kRows(b, true)
}

// BenchmarkReceiveMetadata measures performance of receiving lots of metadata compare to data in rows
func BenchmarkReceiveMetadata(b *testing.B) {
	tb := (*TB)(b)

	// Create a table with 1000 integer fields
	createTableQuery := "CREATE TABLE large_integer_table ("
	for i := 0; i < 1000; i++ {
		createTableQuery += fmt.Sprintf("col_%d INT", i)
		if i < 999 {
			createTableQuery += ", "
		}
	}
	createTableQuery += ")"

	// Initialize database
	db := initDB(b, false,
		"DROP TABLE IF EXISTS large_integer_table",
		createTableQuery,
		"INSERT INTO large_integer_table VALUES ("+
			strings.Repeat("0,", 999)+"0)", // Insert a row of zeros
	)
	defer db.Close()

	b.Run("query", func(b *testing.B) {
		db.SetMaxIdleConns(0)
		db.SetMaxIdleConns(1)

		// Create a slice to scan all columns
		values := make([]any, 1000)
		valuePtrs := make([]any, 1000)
		for j := range values {
			valuePtrs[j] = &values[j]
		}

		// Prepare a SELECT query to retrieve metadata
		stmt := tb.checkStmt(db.Prepare("SELECT * FROM large_integer_table LIMIT 1"))
		defer stmt.Close()

		// Benchmark metadata retrieval
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			rows := tb.checkRows(stmt.Query())

			rows.Next()
			// Scan the row
			err := rows.Scan(valuePtrs...)
			tb.check(err)

			rows.Close()
		}
	})
}

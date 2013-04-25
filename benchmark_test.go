package mysql

import (
	"database/sql"
	"strings"
	"testing"
)

var (
	// dsn from driver_test.go
	sample = []byte(strings.Repeat("0123456789abcdef", 1024*1024))
)

func BenchmarkRoundtripText(b *testing.B) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		b.Fatalf("crashed")
	}
	defer db.Close()
	var result string
	for i := 0; i < b.N; i++ {
		length := 16 + i%(4*b.N)
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
		length := 16 + i%(4*b.N)
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

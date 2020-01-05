package mysql

import (
	"testing"
	"time"
)

func TestNullTime(t *testing.T) {
	runTests(t, dsn+"&parseTime=true&loc=US%2FCentral", func(dbt *DBTest) {
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

		var dbTime NullTime
		err := rows.Scan(&dbTime)
		if err != nil {
			dbt.Fatal("Err", err)
		}

		// Check that dates match
		if reftime.Unix() != dbTime.Time.Unix() {
			dbt.Errorf("times do not match.\n")
			dbt.Errorf(" Now(%v)=%v\n", usCentral, reftime)
			dbt.Errorf(" Now(UTC)=%v\n", dbTime)
		}
		if dbTime.Time.Location().String() != usCentral.String() {
			dbt.Errorf("location do not match.\n")
			dbt.Errorf(" got=%v\n", dbTime.Time.Location())
			dbt.Errorf(" want=%v\n", usCentral)
		}
	})
}

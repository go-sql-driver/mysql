// +build go1.8

package mysql

import (
	"reflect"
	"testing"
)

func TestMultiResultSet(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		type result struct {
			values  [][]int
			columns []string
		}

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

		query := `
SELECT 1 AS col1, 2 AS col2 UNION SELECT 3, 4;
SELECT 0 UNION SELECT 1; -- ignore this result set
SELECT 1 AS col1, 2 AS col2, 3 AS col3 UNION SELECT 4, 5, 6;`

		rows := dbt.mustQuery(query)
		defer rows.Close()

		var res1 result
		for rows.Next() {
			var res [2]int
			if err := rows.Scan(&res[0], &res[1]); err != nil {
				dbt.Fatal(err)
			}
			res1.values = append(res1.values, res[:])
		}

		if rows.Next() {
			dbt.Error("unexpected row")
		}

		cols, err := rows.Columns()
		if err != nil {
			dbt.Fatal(err)
		}
		res1.columns = cols

		if !reflect.DeepEqual(expected[0], res1) {
			dbt.Error("want =", expected[0], "got =", res1)
		}

		if !rows.NextResultSet() {
			dbt.Fatal("expected next result set")
		}

		// ignoring one result set

		if !rows.NextResultSet() {
			dbt.Fatal("expected next result set")
		}

		var res2 result
		cols, err = rows.Columns()
		if err != nil {
			dbt.Fatal(err)
		}
		res2.columns = cols

		for rows.Next() {
			var res [3]int
			if err := rows.Scan(&res[0], &res[1], &res[2]); err != nil {
				dbt.Fatal(err)
			}
			res2.values = append(res2.values, res[:])
		}

		if !reflect.DeepEqual(expected[1], res2) {
			dbt.Error("want =", expected[1], "got =", res2)
		}

		if rows.Next() {
			dbt.Error("unexpected row")
		}

		if rows.NextResultSet() {
			dbt.Error("unexpected next result set")
		}

		if err := rows.Err(); err != nil {
			dbt.Error(err)
		}
	})
}

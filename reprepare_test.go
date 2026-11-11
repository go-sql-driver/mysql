// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2025 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
    "database/sql"
    "testing"
    "time"
)

// Ensures that executing a prepared statement still returns correct data
// after a DDL that changes a column type. This validates automatic
// reprepare on ER_NEED_REPREPARE-capable servers and correctness in general.
func TestPreparedStmtReprepareAfterDDL(t *testing.T) {
    runTests(t, dsn+"&parseTime=true", func(dbt *DBTest) {
        db := dbt.db

        dbt.mustExec("DROP TABLE IF EXISTS reprepare_test")
        dbt.mustExec(`
            CREATE TABLE reprepare_test (
                id INT AUTO_INCREMENT PRIMARY KEY,
                state TINYINT,
                round TINYINT NOT NULL DEFAULT 0,
                remark TEXT,
                ctime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )`)
        t.Cleanup(func() { db.Exec("DROP TABLE IF EXISTS reprepare_test") })

        dbt.mustExec("INSERT INTO reprepare_test(state, round, remark) VALUES (1, 1, 'hello')")

        stmt, err := db.Prepare("SELECT state, round, remark, ctime FROM reprepare_test WHERE id=?")
        if err != nil {
            t.Fatalf("prepare failed: %v", err)
        }
        defer stmt.Close()

        var (
            s1, r1 int
            rem1   string
            ct1    time.Time
        )
        if err := stmt.QueryRow(1).Scan(&s1, &r1, &rem1, &ct1); err != nil {
            t.Fatalf("first scan failed: %v", err)
        }
        if s1 != 1 || r1 != 1 || rem1 != "hello" || ct1.IsZero() {
            t.Fatalf("unexpected first row values: (%d,%d,%q,%v)", s1, r1, rem1, ct1)
        }

        // Change the column type that participates in the prepared statement's result set.
        dbt.mustExec("ALTER TABLE reprepare_test MODIFY state INT")

        var (
            s2, r2 int
            rem2   string
            ct2    time.Time
        )
        // This used to fail or return incorrect data on some servers without reprepare handling.
        if err := stmt.QueryRow(1).Scan(&s2, &r2, &rem2, &ct2); err != nil {
            // Some environments may not reproduce ER_NEED_REPREPARE, so avoid flakiness by surfacing the error.
            t.Fatalf("second scan failed: %v", err)
        }

        if s2 != s1 || r2 != r1 || rem2 != rem1 || ct2.IsZero() {
            t.Fatalf("unexpected second row values after DDL: got (%d,%d,%q,%v), want (%d,%d,%q,<non-zero>)",
                s2, r2, rem2, ct2, s1, r1, rem1,
            )
        }
    })
}

// Validates Exec path also reprovisions the prepared statement after DDL.
func TestPreparedStmtExecReprepareAfterDDL(t *testing.T) {
    runTests(t, dsn, func(dbt *DBTest) {
        db := dbt.db

        dbt.mustExec("DROP TABLE IF EXISTS reprepare_exec_test")
        dbt.mustExec(`
            CREATE TABLE reprepare_exec_test (
                id INT AUTO_INCREMENT PRIMARY KEY,
                value INT NOT NULL
            )`)
        t.Cleanup(func() { db.Exec("DROP TABLE IF EXISTS reprepare_exec_test") })

        stmt, err := db.Prepare("INSERT INTO reprepare_exec_test(value) VALUES (?)")
        if err != nil {
            t.Fatalf("prepare failed: %v", err)
        }
        defer stmt.Close()

        if _, err := stmt.Exec(1); err != nil {
            t.Fatalf("first exec failed: %v", err)
        }

        // Change the column type to trigger metadata invalidation on some servers.
        dbt.mustExec("ALTER TABLE reprepare_exec_test MODIFY value BIGINT")

        if _, err := stmt.Exec(2); err != nil {
            t.Fatalf("second exec (after DDL) failed: %v", err)
        }

        // Verify both rows are present and correct.
        rows := dbt.mustQuery("SELECT value FROM reprepare_exec_test ORDER BY id")
        defer rows.Close()
        var got []int
        for rows.Next() {
            var v int
            if err := rows.Scan(&v); err != nil {
                t.Fatalf("scan values failed: %v", err)
            }
            got = append(got, v)
        }
        if len(got) != 2 || got[0] != 1 || got[1] != 2 {
            t.Fatalf("unexpected values: %v", got)
        }
    })
}

// Ensures repeated scans using the same prepared statement remain correct across DDL, scanning into sql.NullTime.
func TestPreparedStmtReprepareMultipleScansAfterDDL_NullTime(t *testing.T) {
    runTests(t, dsn+"&parseTime=true", func(dbt *DBTest) {
        db := dbt.db

        dbt.mustExec("DROP TABLE IF EXISTS reprepare_multi_test")
        dbt.mustExec(`
            CREATE TABLE reprepare_multi_test (
                id INT AUTO_INCREMENT PRIMARY KEY,
                state TINYINT,
                ctime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )`)
        t.Cleanup(func() { db.Exec("DROP TABLE IF EXISTS reprepare_multi_test") })

        dbt.mustExec("INSERT INTO reprepare_multi_test(state) VALUES (5)")

        stmt, err := db.Prepare("SELECT state, ctime FROM reprepare_multi_test WHERE id=?")
        if err != nil {
            t.Fatalf("prepare failed: %v", err)
        }
        defer stmt.Close()

        // First scan
        {
            var s int
            var ct sql.NullTime
            if err := stmt.QueryRow(1).Scan(&s, &ct); err != nil {
                t.Fatalf("first scan failed: %v", err)
            }
            if s != 5 || !ct.Valid || ct.Time.IsZero() {
                t.Fatalf("unexpected first values: (%d,%v)", s, ct)
            }
        }

        // DDL change that alters one of the selected column types
        dbt.mustExec("ALTER TABLE reprepare_multi_test MODIFY state INT")

        // Second scan after DDL
        {
            var s int
            var ct sql.NullTime
            if err := stmt.QueryRow(1).Scan(&s, &ct); err != nil {
                t.Fatalf("second scan failed: %v", err)
            }
            if s != 5 || !ct.Valid || ct.Time.IsZero() {
                t.Fatalf("unexpected second values after DDL: (%d,%v)", s, ct)
            }
        }

        // Third scan to ensure continued usability
        {
            var s int
            var ct sql.NullTime
            if err := stmt.QueryRow(1).Scan(&s, &ct); err != nil {
                t.Fatalf("third scan failed: %v", err)
            }
            if s != 5 || !ct.Valid || ct.Time.IsZero() {
                t.Fatalf("unexpected third values after DDL: (%d,%v)", s, ct)
            }
        }
    })
}



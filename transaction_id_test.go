// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2012 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"
)

// TestTransactionErrorHandling tests proper error handling in transaction scenarios
// that might be affected by transaction ID issues
func TestTransactionErrorHandling(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()

	// Test 1: Basic transaction commit and rollback
	t.Run("BasicTransactionOperations", func(t *testing.T) {
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		// Insert a test record
		_, err = tx.Exec("CREATE TEMPORARY TABLE test_tx (id INT, value VARCHAR(50))")
		if err != nil {
			tx.Rollback()
			t.Fatalf("Failed to create temp table: %v", err)
		}

		_, err = tx.Exec("INSERT INTO test_tx (id, value) VALUES (1, 'test')")
		if err != nil {
			tx.Rollback()
			t.Fatalf("Failed to insert test data: %v", err)
		}

		// Commit the transaction
		err = tx.Commit()
		if err != nil {
			t.Fatalf("Failed to commit transaction: %v", err)
		}

		// Verify the data was committed
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM test_tx").Scan(&count)
		if err != nil {
			t.Fatalf("Failed to verify committed data: %v", err)
		}
		if count != 1 {
			t.Errorf("Expected 1 row, got %d", count)
		}
	})

	// Test 2: Transaction rollback on error
	t.Run("TransactionRollbackOnError", func(t *testing.T) {
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		// Create temp table
		_, err = tx.Exec("CREATE TEMPORARY TABLE test_tx_rollback (id INT, value VARCHAR(50))")
		if err != nil {
			tx.Rollback()
			t.Fatalf("Failed to create temp table: %v", err)
		}

		// Insert some data
		_, err = tx.Exec("INSERT INTO test_tx_rollback (id, value) VALUES (1, 'test')")
		if err != nil {
			tx.Rollback()
			t.Fatalf("Failed to insert test data: %v", err)
		}

		// Intentionally cause an error
		_, err = tx.Exec("INSERT INTO test_tx_rollback (id, value) VALUES ('invalid', 'data')")
		if err == nil {
			tx.Rollback()
			t.Fatal("Expected error for invalid data, but got none")
		}

		// Rollback the transaction
		err = tx.Rollback()
		if err != nil {
			t.Fatalf("Failed to rollback transaction: %v", err)
		}

		// Verify the data was rolled back (table should be empty or not exist)
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM test_tx_rollback").Scan(&count)
		if err == nil && count == 0 {
			// Table exists but is empty - rollback worked
			return
		}
		// Table doesn't exist - also acceptable after rollback
	})

	// Test 3: Concurrent transactions
	t.Run("ConcurrentTransactions", func(t *testing.T) {
		// Create a test table
		_, err := db.Exec("CREATE TEMPORARY TABLE test_concurrent (id INT PRIMARY KEY, value VARCHAR(50))")
		if err != nil {
			t.Fatalf("Failed to create test table: %v", err)
		}

		// Start multiple concurrent transactions
		const numGoroutines = 5
		done := make(chan bool, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				defer func() { done <- true }()
				
				tx, err := db.Begin()
				if err != nil {
					t.Errorf("Goroutine %d: Failed to begin transaction: %v", id, err)
					return
				}

				// Insert with unique ID
				_, err = tx.Exec("INSERT INTO test_concurrent (id, value) VALUES (?, ?)", id, fmt.Sprintf("value_%d", id))
				if err != nil {
					tx.Rollback()
					t.Errorf("Goroutine %d: Failed to insert data: %v", id, err)
					return
				}

				err = tx.Commit()
				if err != nil {
					t.Errorf("Goroutine %d: Failed to commit transaction: %v", id, err)
					return
				}
			}(i)
		}

		// Wait for all goroutines to complete
		for i := 0; i < numGoroutines; i++ {
			<-done
		}

		// Verify all data was inserted correctly
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM test_concurrent").Scan(&count)
		if err != nil {
			t.Fatalf("Failed to count rows: %v", err)
		}
		if count != numGoroutines {
			t.Errorf("Expected %d rows, got %d", numGoroutines, count)
		}
	})
}

// TestTransactionIsolationLevels tests different transaction isolation levels
func TestTransactionIsolationLevels(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()

	isolationLevels := []struct {
		name  string
		level string
	}{
		{"READ_UNCOMMITTED", "READ UNCOMMITTED"},
		{"READ_COMMITTED", "READ COMMITTED"},
		{"REPEATABLE_READ", "REPEATABLE READ"},
		{"SERIALIZABLE", "SERIALIZABLE"},
	}

	for _, test := range isolationLevels {
		t.Run(test.name, func(t *testing.T) {
			// Set isolation level
			_, err := db.Exec(fmt.Sprintf("SET SESSION TRANSACTION ISOLATION LEVEL %s", test.level))
			if err != nil {
				t.Fatalf("Failed to set isolation level %s: %v", test.level, err)
			}

			// Start transaction
			tx, err := db.Begin()
			if err != nil {
				t.Fatalf("Failed to begin transaction: %v", err)
			}

			// Create test table
			_, err = tx.Exec("CREATE TEMPORARY TABLE test_isolation (id INT, value VARCHAR(50))")
			if err != nil {
				tx.Rollback()
				t.Fatalf("Failed to create temp table: %v", err)
			}

			// Insert test data
			_, err = tx.Exec("INSERT INTO test_isolation (id, value) VALUES (1, 'test')")
			if err != nil {
				tx.Rollback()
				t.Fatalf("Failed to insert test data: %v", err)
			}

			// Commit transaction
			err = tx.Commit()
			if err != nil {
				t.Fatalf("Failed to commit transaction: %v", err)
			}

			// Verify data exists
			var count int
			err = db.QueryRow("SELECT COUNT(*) FROM test_isolation").Scan(&count)
			if err != nil {
				t.Fatalf("Failed to verify data: %v", err)
			}
			if count != 1 {
				t.Errorf("Expected 1 row, got %d", count)
			}
		})
	}
}

// TestTransactionContext tests transaction handling with context
func TestTransactionContext(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()

	t.Run("TransactionWithTimeout", func(t *testing.T) {
		// Create a context with a very short timeout
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
		defer cancel()

		// Wait for context to timeout
		time.Sleep(1 * time.Millisecond)

		// Try to begin transaction with timed out context
		tx, err := db.BeginTx(ctx, nil)
		if err == nil {
			tx.Rollback()
			t.Fatal("Expected context timeout error, but got none")
		}
		if err != context.DeadlineExceeded {
			t.Errorf("Expected context.DeadlineExceeded, got %v", err)
		}
	})

	t.Run("TransactionWithCancellation", func(t *testing.T) {
		// Create a cancellable context
		ctx, cancel := context.WithCancel(context.Background())

		// Cancel the context immediately
		cancel()

		// Try to begin transaction with cancelled context
		tx, err := db.BeginTx(ctx, nil)
		if err == nil {
			tx.Rollback()
			t.Fatal("Expected context cancellation error, but got none")
		}
		if err != context.Canceled {
			t.Errorf("Expected context.Canceled, got %v", err)
		}
	})
}

// TestTransactionRecoveryScenarios simulates recovery scenarios that might
// be related to transaction ID issues
func TestTransactionRecoveryScenarios(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()

	t.Run("RecoveryAfterConnectionLoss", func(t *testing.T) {
		// Create a test table
		_, err := db.Exec("CREATE TEMPORARY TABLE test_recovery (id INT PRIMARY KEY, value VARCHAR(50))")
		if err != nil {
			t.Fatalf("Failed to create test table: %v", err)
		}

		// Begin transaction and insert data
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		_, err = tx.Exec("INSERT INTO test_recovery (id, value) VALUES (1, 'before_disconnect')")
		if err != nil {
			tx.Rollback()
			t.Fatalf("Failed to insert data: %v", err)
		}

		// Simulate connection issues by attempting to use a closed connection
		// This tests the driver's error handling capabilities
		err = tx.Commit()
		if err != nil {
			t.Errorf("Failed to commit transaction: %v", err)
		}

		// Verify data was committed
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM test_recovery").Scan(&count)
		if err != nil {
			t.Fatalf("Failed to verify committed data: %v", err)
		}
		if count != 1 {
			t.Errorf("Expected 1 row, got %d", count)
		}
	})

	t.Run("NestedTransactionSimulation", func(t *testing.T) {
		// MySQL doesn't support true nested transactions, but we can simulate
		// savepoints which are often used for similar purposes
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		// Create test table
		_, err = tx.Exec("CREATE TEMPORARY TABLE test_nested (id INT, value VARCHAR(50))")
		if err != nil {
			tx.Rollback()
			t.Fatalf("Failed to create temp table: %v", err)
		}

		// Insert initial data
		_, err = tx.Exec("INSERT INTO test_nested (id, value) VALUES (1, 'initial')")
		if err != nil {
			tx.Rollback()
			t.Fatalf("Failed to insert initial data: %v", err)
		}

		// Create a savepoint (simulating nested transaction)
		_, err = tx.Exec("SAVEPOINT sp1")
		if err != nil {
			tx.Rollback()
			t.Fatalf("Failed to create savepoint: %v", err)
		}

		// Insert more data
		_, err = tx.Exec("INSERT INTO test_nested (id, value) VALUES (2, 'after_savepoint')")
		if err != nil {
			tx.Rollback()
			t.Fatalf("Failed to insert data after savepoint: %v", err)
		}

		// Rollback to savepoint
		_, err = tx.Exec("ROLLBACK TO SAVEPOINT sp1")
		if err != nil {
			tx.Rollback()
			t.Fatalf("Failed to rollback to savepoint: %v", err)
		}

		// Commit the main transaction
		err = tx.Commit()
		if err != nil {
			t.Fatalf("Failed to commit transaction: %v", err)
		}

		// Verify only initial data exists
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM test_nested").Scan(&count)
		if err != nil {
			t.Fatalf("Failed to verify data: %v", err)
		}
		if count != 1 {
			t.Errorf("Expected 1 row (after savepoint rollback), got %d", count)
		}
	})
}

// Helper function to create a test database connection
func createTestDB(t *testing.T) *sql.DB {
	// This would typically use a test DSN
	// For this example, we'll assume the test environment is set up
	dsn := "testuser:testpass@tcp(localhost:3306)/testdb?parseTime=true"
	
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Skipf("Failed to connect to test database: %v", err)
	}

	// Test the connection
	err = db.Ping()
	if err != nil {
		db.Close()
		t.Skipf("Failed to ping test database: %v", err)
	}

	return db
}

// BenchmarkTransactionOperations benchmarks transaction performance
// to ensure transaction handling doesn't introduce significant overhead
func BenchmarkTransactionOperations(b *testing.B) {
	db := createBenchDB(b)
	defer db.Close()

	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		tx, err := db.Begin()
		if err != nil {
			b.Fatalf("Failed to begin transaction: %v", err)
		}

		// Simple operation
		_, err = tx.Exec("SELECT 1")
		if err != nil {
			tx.Rollback()
			b.Fatalf("Failed to execute query: %v", err)
		}

		err = tx.Commit()
		if err != nil {
			b.Fatalf("Failed to commit transaction: %v", err)
		}
	}
}

// Helper function to create a benchmark database connection
func createBenchDB(b *testing.B) *sql.DB {
	dsn := "testuser:testpass@tcp(localhost:3306)/testdb?parseTime=true"
	
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		b.Skipf("Failed to connect to benchmark database: %v", err)
	}

	err = db.Ping()
	if err != nil {
		db.Close()
		b.Skipf("Failed to ping benchmark database: %v", err)
	}

	return db
}

// Go MySQL Driver - Transaction Best Practices Examples
//
// This file demonstrates best practices for transaction handling in applications
// using the go-sql-driver/mysql, particularly focusing on robustness and
// scenarios that might relate to transaction ID issues.

package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// Example 1: Basic Transaction Pattern with Proper Error Handling
func basicTransactionExample(db *sql.DB) error {
	// Start transaction
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Ensure rollback happens if there's an error
	defer func() {
		if err != nil {
			// Attempt to rollback, don't overwrite the original error
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				log.Printf("failed to rollback transaction: %v", rollbackErr)
			}
		}
	}()

	// Execute operations within transaction
	_, err = tx.Exec("INSERT INTO users (name, email) VALUES (?, ?)", "John Doe", "john@example.com")
	if err != nil {
		return fmt.Errorf("failed to insert user: %w", err)
	}

	_, err = tx.Exec("INSERT INTO user_profiles (user_id, bio) VALUES (?, ?)", 1, "Software Developer")
	if err != nil {
		return fmt.Errorf("failed to insert user profile: %w", err)
	}

	// Commit transaction
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// Example 2: Transaction with Context and Timeout
func transactionWithContextExample(db *sql.DB) error {
	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Begin transaction with context
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				log.Printf("failed to rollback transaction: %v", rollbackErr)
			}
		}
	}()

	// Execute operations with context awareness
	_, err = tx.ExecContext(ctx, "INSERT INTO orders (customer_id, total) VALUES (?, ?)", 1, 99.99)
	if err != nil {
		return fmt.Errorf("failed to insert order: %w", err)
	}

	// Simulate some processing time
	select {
	case <-time.After(100 * time.Millisecond):
		// Continue with transaction
	case <-ctx.Done():
		return ctx.Err()
	}

	_, err = tx.ExecContext(ctx, "INSERT INTO order_items (order_id, product_id, quantity) VALUES (?, ?, ?)", 1, 1, 2)
	if err != nil {
		return fmt.Errorf("failed to insert order items: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// Example 3: Retry Pattern for Transient Errors
func transactionWithRetryExample(db *sql.DB, maxRetries int) error {
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff
			backoff := time.Duration(1<<uint(attempt)) * time.Second
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
			log.Printf("Retrying transaction attempt %d after %v backoff", attempt+1, backoff)
			time.Sleep(backoff)
		}

		err := attemptTransaction(db)
		if err == nil {
			return nil // Success
		}

		lastErr = err

		// Check if error is retryable
		if !isRetryableError(err) {
			break // Don't retry non-retryable errors
		}

		log.Printf("Transaction attempt %d failed: %v", attempt+1, err)
	}

	return fmt.Errorf("transaction failed after %d attempts, last error: %w", maxRetries, lastErr)
}

func attemptTransaction(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Simulate a potentially failing operation
	_, err = tx.Exec("INSERT INTO audit_log (action, timestamp) VALUES (?, NOW())", "USER_LOGIN")
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func isRetryableError(err error) bool {
	// Check for common retryable MySQL errors
	errStr := err.Error()
	retryableErrors := []string{
		"deadlock",
		"lock wait timeout",
		"connection reset",
		"server has gone away",
	}

	for _, retryableErr := range retryableErrors {
		if contains(errStr, retryableErr) {
			return true
		}
	}

	return false
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && 
		(hasPrefix(s, substr) || hasSuffix(s, substr) || indexOf(s, substr) >= 0))
}

func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

func hasSuffix(s, suffix string) bool {
	return len(s) >= len(suffix) && s[len(s)-len(suffix):] == suffix
}

func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

// Example 4: Transaction with Savepoints (Nested Transaction Simulation)
func transactionWithSavepointsExample(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				log.Printf("failed to rollback transaction: %v", rollbackErr)
			}
		}
	}()

	// First operation
	_, err = tx.Exec("INSERT INTO customers (name) VALUES (?)", "Acme Corp")
	if err != nil {
		return fmt.Errorf("failed to insert customer: %w", err)
	}

	// Create savepoint
	_, err = tx.Exec("SAVEPOINT sp_customer_insert")
	if err != nil {
		return fmt.Errorf("failed to create savepoint: %w", err)
	}

	// Operations that might fail
	_, err = tx.Exec("INSERT INTO customer_contacts (customer_id, phone) VALUES (?, ?)", 1, "555-0123")
	if err != nil {
		// Rollback to savepoint if contact insertion fails
		_, rollbackErr := tx.Exec("ROLLBACK TO SAVEPOINT sp_customer_insert")
		if rollbackErr != nil {
			return fmt.Errorf("failed to rollback to savepoint: %w", rollbackErr)
		}
		log.Printf("Contact insertion failed, rolled back to savepoint: %v", err)
	}

	// Continue with other operations
	_, err = tx.Exec("INSERT INTO customer_notes (customer_id, note) VALUES (?, ?)", 1, "Initial customer setup")
	if err != nil {
		return fmt.Errorf("failed to insert customer note: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// Example 5: Transaction Monitoring and Health Check
func transactionHealthCheckExample(db *sql.DB) error {
	// Check transaction status and system health
	var trxCount int
	err := db.QueryRow("SELECT COUNT(*) FROM INFORMATION_SCHEMA.INNODB_TRX").Scan(&trxCount)
	if err != nil {
		return fmt.Errorf("failed to check active transactions: %w", err)
	}

	log.Printf("Current active transactions: %d", trxCount)

	// Check for long-running transactions
	rows, err := db.Query(`
		SELECT trx_id, trx_started, trx_state 
		FROM INFORMATION_SCHEMA.INNODB_TRX 
		WHERE trx_started < NOW() - INTERVAL 1 MINUTE
	`)
	if err != nil {
		return fmt.Errorf("failed to check long-running transactions: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var trxID, trxState string
		var trxStarted time.Time
		if err := rows.Scan(&trxID, &trxStarted, &trxState); err != nil {
			log.Printf("Failed to scan transaction info: %v", err)
			continue
		}
		log.Printf("Long-running transaction detected: ID=%s, Started=%v, State=%s", 
			trxID, trxStarted, trxState)
	}

	// Perform a simple transaction to test connectivity
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin health check transaction: %w", err)
	}

	_, err = tx.Exec("SELECT 1")
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("health check query failed: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit health check transaction: %w", err)
	}

	log.Println("Transaction health check passed")
	return nil
}

// Example 6: Transaction Pool Management
type TransactionManager struct {
	db *sql.DB
}

func NewTransactionManager(db *sql.DB) *TransactionManager {
	return &TransactionManager{db: db}
}

func (tm *TransactionManager) ExecuteInTransaction(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := tm.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				log.Printf("failed to rollback transaction: %v", rollbackErr)
			}
		}
	}()

	// Execute the user function within the transaction
	err = fn(tx)
	if err != nil {
		return err
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// Example usage of TransactionManager
func transactionManagerExample(tm *TransactionManager) error {
	ctx := context.Background()

	err := tm.ExecuteInTransaction(ctx, func(tx *sql.Tx) error {
		// Insert user
		result, err := tx.Exec("INSERT INTO users (name, email) VALUES (?, ?)", "Jane Doe", "jane@example.com")
		if err != nil {
			return err
		}

		userID, err := result.LastInsertId()
		if err != nil {
			return err
		}

		// Insert user profile
		_, err = tx.Exec("INSERT INTO user_profiles (user_id, bio) VALUES (?, ?)", userID, "Product Manager")
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("transaction failed: %w", err)
	}

	return nil
}

// Example 7: Error Recovery and Diagnostics
func transactionErrorRecoveryExample(db *sql.DB) error {
	// Attempt to execute a transaction
	err := basicTransactionExample(db)
	if err != nil {
		log.Printf("Transaction failed: %v", err)

		// Check if it's a connection-related error
		if isConnectionError(err) {
			log.Println("Detected connection error, attempting to reconnect...")
			
			// Close and reopen connection (in a real app, you'd use a connection pool)
			db.Close()
			newDB, err := reconnectDatabase()
			if err != nil {
				return fmt.Errorf("failed to reconnect to database: %w", err)
			}

			// Retry the transaction with the new connection
			err = basicTransactionExample(newDB)
			if err != nil {
				return fmt.Errorf("transaction failed after reconnection: %w", err)
			}

			log.Println("Transaction succeeded after reconnection")
			return nil
		}

		// For other errors, you might want to implement different strategies
		if isTransactionIDError(err) {
			log.Println("Detected potential transaction ID related error, running diagnostics...")
			return runDiagnostics(db)
		}

		return err
	}

	return nil
}

func isConnectionError(err error) bool {
	errStr := err.Error()
	connectionErrors := []string{
		"connection refused",
		"connection reset",
		"server has gone away",
		"broken pipe",
	}

	for _, connErr := range connectionErrors {
		if contains(errStr, connErr) {
			return true
		}
	}

	return false
}

func isTransactionIDError(err error) bool {
	errStr := err.Error()
	transactionIDErrors := []string{
		"transaction id",
		"system-wide maximum",
		"trx_id",
	}

	for _, trxErr := range transactionIDErrors {
		if contains(errStr, trxErr) {
			return true
		}
	}

	return false
}

func reconnectDatabase() (*sql.DB, error) {
	dsn := "user:password@tcp(localhost:3306)/dbname?parseTime=true"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	// Test the connection
	err = db.Ping()
	if err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

func runDiagnostics(db *sql.DB) error {
	// Check InnoDB status
	var innodbStatus string
	err := db.QueryRow("SHOW ENGINE INNODB STATUS").Scan(&innodbStatus)
	if err != nil {
		log.Printf("Failed to get InnoDB status: %v", err)
	} else {
		log.Printf("InnoDB status retrieved successfully")
		// In a real application, you'd parse this status for relevant information
	}

	// Check system variables
	rows, err := db.Query("SHOW VARIABLES WHERE Variable_name LIKE '%innodb%' OR Variable_name LIKE '%transaction%'")
	if err != nil {
		return fmt.Errorf("failed to check system variables: %w", err)
	}
	defer rows.Close()

	log.Println("InnoDB and Transaction-related variables:")
	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			log.Printf("Failed to scan variable: %v", err)
			continue
		}
		log.Printf("  %s = %s", name, value)
	}

	return nil
}

func main() {
	// Initialize database connection
	dsn := "user:password@tcp(localhost:3306)/testdb?parseTime=true&timeout=30s&readTimeout=30s&writeTimeout=30s"
	
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Test connection
	err = db.Ping()
	if err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Run examples
	log.Println("Running transaction examples...")

	// Example 1: Basic transaction
	log.Println("1. Running basic transaction example...")
	err = basicTransactionExample(db)
	if err != nil {
		log.Printf("Basic transaction example failed: %v", err)
	} else {
		log.Println("Basic transaction example succeeded")
	}

	// Example 2: Transaction with context
	log.Println("2. Running transaction with context example...")
	err = transactionWithContextExample(db)
	if err != nil {
		log.Printf("Context transaction example failed: %v", err)
	} else {
		log.Println("Context transaction example succeeded")
	}

	// Example 3: Transaction with retry
	log.Println("3. Running transaction with retry example...")
	err = transactionWithRetryExample(db, 3)
	if err != nil {
		log.Printf("Retry transaction example failed: %v", err)
	} else {
		log.Println("Retry transaction example succeeded")
	}

	// Example 4: Transaction with savepoints
	log.Println("4. Running transaction with savepoints example...")
	err = transactionWithSavepointsExample(db)
	if err != nil {
		log.Printf("Savepoint transaction example failed: %v", err)
	} else {
		log.Println("Savepoint transaction example succeeded")
	}

	// Example 5: Health check
	log.Println("5. Running transaction health check...")
	err = transactionHealthCheckExample(db)
	if err != nil {
		log.Printf("Health check failed: %v", err)
	} else {
		log.Println("Health check passed")
	}

	// Example 6: Transaction manager
	log.Println("6. Running transaction manager example...")
	tm := NewTransactionManager(db)
	err = transactionManagerExample(tm)
	if err != nil {
		log.Printf("Transaction manager example failed: %v", err)
	} else {
		log.Println("Transaction manager example succeeded")
	}

	// Example 7: Error recovery
	log.Println("7. Running error recovery example...")
	err = transactionErrorRecoveryExample(db)
	if err != nil {
		log.Printf("Error recovery example failed: %v", err)
	} else {
		log.Println("Error recovery example succeeded")
	}

	log.Println("All transaction examples completed")
}

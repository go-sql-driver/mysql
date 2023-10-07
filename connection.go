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
	"database/sql/driver"
	"encoding/json"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// aLongTimeAgo is a non-zero time, far in the past, used for
// immediate cancellation of dials.
var aLongTimeAgo = time.Unix(1, 0)

type readResult struct {
	data []byte
	err  error
}

type writeResult struct {
	n   int
	err error
}

type mysqlConn struct {
	muRead           sync.Mutex // protects netConn for reads
	netConn          net.Conn
	rawConn          net.Conn    // underlying connection when netConn is TLS connection.
	result           mysqlResult // managed by clearResult() and handleOkPacket().
	cfg              *Config
	connector        *connector
	maxAllowedPacket int
	maxWriteSize     int
	readTimeout      time.Duration
	writeTimeout     time.Duration
	flags            clientFlag
	status           statusFlag
	sequence         uint8
	parseTime        bool
	reset            bool // set when the Go SQL package calls ResetSession

	// for context support (Go 1.8+)
	closech chan struct{}
	closed  atomicBool // set when conn is closed, before closech is closed

	data     [16]byte // buffer for small writes
	readBuf  []byte
	readRes  chan readResult  // channel for read result
	writeReq chan []byte      // buffered channel for write packets
	writeRes chan writeResult // channel for write result
}

// Handles parameters set in DSN after the connection is established
func (mc *mysqlConn) handleParams(ctx context.Context) (err error) {
	var cmdSet strings.Builder

	for param, val := range mc.cfg.Params {
		switch param {
		// Charset: character_set_connection, character_set_client, character_set_results
		case "charset":
			charsets := strings.Split(val, ",")
			for _, cs := range charsets {
				// ignore errors here - a charset may not exist
				if mc.cfg.Collation != "" {
					err = mc.exec(ctx, "SET NAMES "+cs+" COLLATE "+mc.cfg.Collation)
				} else {
					err = mc.exec(ctx, "SET NAMES "+cs)
				}
				if err == nil {
					break
				}
			}
			if err != nil {
				return
			}

		// Other system vars accumulated in a single SET command
		default:
			if cmdSet.Len() == 0 {
				// Heuristic: 29 chars for each other key=value to reduce reallocations
				cmdSet.Grow(4 + len(param) + 3 + len(val) + 30*(len(mc.cfg.Params)-1))
				cmdSet.WriteString("SET ")
			} else {
				cmdSet.WriteString(", ")
			}
			cmdSet.WriteString(param)
			cmdSet.WriteString(" = ")
			cmdSet.WriteString(val)
		}
	}

	if cmdSet.Len() > 0 {
		err = mc.exec(ctx, cmdSet.String())
		if err != nil {
			return
		}
	}

	return
}

func (mc *mysqlConn) markBadConn(err error) error {
	if mc == nil {
		return err
	}
	if err != errBadConnNoWrite {
		return err
	}
	return driver.ErrBadConn
}

func (mc *mysqlConn) Begin() (driver.Tx, error) {
	return mc.begin(context.Background(), false)
}

func (mc *mysqlConn) begin(ctx context.Context, readOnly bool) (driver.Tx, error) {
	if mc.closed.Load() {
		mc.cfg.Logger.Print(ErrInvalidConn)
		return nil, driver.ErrBadConn
	}
	var q string
	if readOnly {
		q = "START TRANSACTION READ ONLY"
	} else {
		q = "START TRANSACTION"
	}
	err := mc.exec(ctx, q)
	if err == nil {
		return &mysqlTx{
			ctx: ctx,
			mc:  mc,
		}, err
	}
	return nil, mc.markBadConn(err)
}

func (mc *mysqlConn) Close() error {
	return mc.closeContext(context.Background())
}

func (mc *mysqlConn) closeContext(ctx context.Context) (err error) {
	// Makes Close idempotent
	if !mc.closed.Load() {
		err = mc.writeCommandPacket(context.Background(), comQuit)
	}

	mc.cleanup()

	return
}

// Closes the network connection and unsets internal variables. Do not call this
// function after successfully authentication, call Close instead. This function
// is called before auth or on auth failure because MySQL will have already
// closed the network connection.
func (mc *mysqlConn) cleanup() {
	if mc.closed.Swap(true) {
		return
	}

	// Makes cleanup idempotent
	close(mc.closech)
	if mc.netConn == nil {
		return
	}
	if err := mc.netConn.Close(); err != nil {
		mc.cfg.Logger.Print(err)
	}
	mc.clearResult()
}

func (mc *mysqlConn) error() error {
	if mc.closed.Load() {
		return ErrInvalidConn
	}
	return nil
}

func (mc *mysqlConn) Prepare(query string) (driver.Stmt, error) {
	return mc.PrepareContext(context.Background(), query)
}

func (mc *mysqlConn) interpolateParams(query string, args []driver.Value) (string, error) {
	// Number of ? should be same to len(args)
	if strings.Count(query, "?") != len(args) {
		return "", driver.ErrSkip
	}

	var err error
	buf := make([]byte, 0, len(query))
	argPos := 0

	for i := 0; i < len(query); i++ {
		q := strings.IndexByte(query[i:], '?')
		if q == -1 {
			buf = append(buf, query[i:]...)
			break
		}
		buf = append(buf, query[i:i+q]...)
		i += q

		arg := args[argPos]
		argPos++

		if arg == nil {
			buf = append(buf, "NULL"...)
			continue
		}

		switch v := arg.(type) {
		case int64:
			buf = strconv.AppendInt(buf, v, 10)
		case uint64:
			// Handle uint64 explicitly because our custom ConvertValue emits unsigned values
			buf = strconv.AppendUint(buf, v, 10)
		case float64:
			buf = strconv.AppendFloat(buf, v, 'g', -1, 64)
		case bool:
			if v {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case time.Time:
			if v.IsZero() {
				buf = append(buf, "'0000-00-00'"...)
			} else {
				buf = append(buf, '\'')
				buf, err = appendDateTime(buf, v.In(mc.cfg.Loc))
				if err != nil {
					return "", err
				}
				buf = append(buf, '\'')
			}
		case json.RawMessage:
			buf = append(buf, '\'')
			if mc.status&statusNoBackslashEscapes == 0 {
				buf = escapeBytesBackslash(buf, v)
			} else {
				buf = escapeBytesQuotes(buf, v)
			}
			buf = append(buf, '\'')
		case []byte:
			if v == nil {
				buf = append(buf, "NULL"...)
			} else {
				buf = append(buf, "_binary'"...)
				if mc.status&statusNoBackslashEscapes == 0 {
					buf = escapeBytesBackslash(buf, v)
				} else {
					buf = escapeBytesQuotes(buf, v)
				}
				buf = append(buf, '\'')
			}
		case string:
			buf = append(buf, '\'')
			if mc.status&statusNoBackslashEscapes == 0 {
				buf = escapeStringBackslash(buf, v)
			} else {
				buf = escapeStringQuotes(buf, v)
			}
			buf = append(buf, '\'')
		default:
			return "", driver.ErrSkip
		}

		if len(buf)+4 > mc.maxAllowedPacket {
			return "", driver.ErrSkip
		}
	}
	if argPos != len(args) {
		return "", driver.ErrSkip
	}
	return string(buf), nil
}

func (mc *mysqlConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	ctx := context.Background()

	if mc.closed.Load() {
		mc.cfg.Logger.Print(ErrInvalidConn)
		return nil, driver.ErrBadConn
	}
	if len(args) != 0 {
		if !mc.cfg.InterpolateParams {
			return nil, driver.ErrSkip
		}
		// try to interpolate the parameters to save extra roundtrips for preparing and closing a statement
		prepared, err := mc.interpolateParams(query, args)
		if err != nil {
			return nil, err
		}
		query = prepared
	}

	err := mc.exec(ctx, query)
	if err == nil {
		copied := mc.result
		return &copied, err
	}
	return nil, mc.markBadConn(err)
}

// Internal function to execute commands
func (mc *mysqlConn) exec(ctx context.Context, query string) error {
	handleOk := mc.clearResult()
	// Send command
	if err := mc.writeCommandPacketStr(ctx, comQuery, query); err != nil {
		return mc.markBadConn(err)
	}

	// Read Result
	resLen, err := handleOk.readResultSetHeaderPacket(ctx)
	if err != nil {
		return err
	}

	if resLen > 0 {
		// columns
		if err := mc.readUntilEOF(ctx); err != nil {
			return err
		}

		// rows
		if err := mc.readUntilEOF(ctx); err != nil {
			return err
		}
	}

	return handleOk.discardResults(ctx)
}

func (mc *mysqlConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	return mc.query(context.Background(), query, args)
}

func (mc *mysqlConn) query(ctx context.Context, query string, args []driver.Value) (*textRows, error) {
	handleOk := mc.clearResult()

	if mc.closed.Load() {
		mc.cfg.Logger.Print(ErrInvalidConn)
		return nil, driver.ErrBadConn
	}
	if len(args) != 0 {
		if !mc.cfg.InterpolateParams {
			return nil, driver.ErrSkip
		}
		// try client-side prepare to reduce roundtrip
		prepared, err := mc.interpolateParams(query, args)
		if err != nil {
			return nil, err
		}
		query = prepared
	}
	// Send command
	err := mc.writeCommandPacketStr(ctx, comQuery, query)
	if err == nil {
		// Read Result
		var resLen int
		resLen, err = handleOk.readResultSetHeaderPacket(ctx)
		if err == nil {
			rows := new(textRows)
			rows.mc = mc
			rows.ctx = ctx

			if resLen == 0 {
				rows.rs.done = true

				switch err := rows.NextResultSet(); err {
				case nil, io.EOF:
					return rows, nil
				default:
					return nil, err
				}
			}

			// Columns
			rows.rs.columns, err = mc.readColumns(ctx, resLen)
			return rows, err
		}
	}
	return nil, mc.markBadConn(err)
}

// Gets the value of the given MySQL System Variable
// The returned byte slice is only valid until the next read
func (mc *mysqlConn) getSystemVar(ctx context.Context, name string) ([]byte, error) {
	// Send command
	handleOk := mc.clearResult()
	if err := mc.writeCommandPacketStr(ctx, comQuery, "SELECT @@"+name); err != nil {
		return nil, err
	}

	// Read Result
	resLen, err := handleOk.readResultSetHeaderPacket(ctx)
	if err == nil {
		rows := new(textRows)
		rows.mc = mc
		rows.rs.columns = []mysqlField{{fieldType: fieldTypeVarChar}}

		if resLen > 0 {
			// Columns
			if err := mc.readUntilEOF(ctx); err != nil {
				return nil, err
			}
		}

		dest := make([]driver.Value, resLen)
		if err = rows.readRow(dest); err == nil {
			return dest[0].([]byte), mc.readUntilEOF(ctx)
		}
	}
	return nil, err
}

// Ping implements driver.Pinger interface
func (mc *mysqlConn) Ping(ctx context.Context) (err error) {
	if mc.closed.Load() {
		mc.cfg.Logger.Print(ErrInvalidConn)
		return driver.ErrBadConn
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	handleOk := mc.clearResult()
	if err = mc.writeCommandPacket(ctx, comPing); err != nil {
		return mc.markBadConn(err)
	}

	return handleOk.readResultOK(ctx)
}

// BeginTx implements driver.ConnBeginTx interface
func (mc *mysqlConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if mc.closed.Load() {
		return nil, driver.ErrBadConn
	}

	if sql.IsolationLevel(opts.Isolation) != sql.LevelDefault {
		level, err := mapIsolationLevel(opts.Isolation)
		if err != nil {
			return nil, err
		}
		err = mc.exec(ctx, "SET TRANSACTION ISOLATION LEVEL "+level)
		if err != nil {
			return nil, err
		}
	}

	return mc.begin(ctx, opts.ReadOnly)
}

func (mc *mysqlConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}

	rows, err := mc.query(ctx, query, dargs)
	if err != nil {
		return nil, err
	}
	return rows, err
}

func (mc *mysqlConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if mc.closed.Load() {
		mc.cfg.Logger.Print(ErrInvalidConn)
		return nil, driver.ErrBadConn
	}

	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}
	if len(dargs) != 0 {
		if !mc.cfg.InterpolateParams {
			return nil, driver.ErrSkip
		}
		// try to interpolate the parameters to save extra roundtrips for preparing and closing a statement
		prepared, err := mc.interpolateParams(query, dargs)
		if err != nil {
			return nil, err
		}
		query = prepared
	}

	err = mc.exec(ctx, query)
	if err == nil {
		copied := mc.result
		return &copied, err
	}
	return nil, mc.markBadConn(err)
}

func (mc *mysqlConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if mc.closed.Load() {
		mc.cfg.Logger.Print(ErrInvalidConn)
		return nil, driver.ErrBadConn
	}
	// Send command
	err := mc.writeCommandPacketStr(ctx, comStmtPrepare, query)
	if err != nil {
		// STMT_PREPARE is safe to retry.  So we can return ErrBadConn here.
		mc.cfg.Logger.Print(err)
		return nil, driver.ErrBadConn
	}

	stmt := &mysqlStmt{
		mc: mc,
	}

	// Read Result
	columnCount, err := stmt.readPrepareResultPacket(ctx)
	if err == nil {
		if stmt.paramCount > 0 {
			if err = mc.readUntilEOF(ctx); err != nil {
				return nil, err
			}
		}

		if columnCount > 0 {
			err = mc.readUntilEOF(ctx)
		}
	}

	return stmt, err
}

func (stmt *mysqlStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}
	return stmt.query(ctx, dargs)
}

func (stmt *mysqlStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if stmt.mc.closed.Load() {
		stmt.mc.cfg.Logger.Print(ErrInvalidConn)
		return nil, driver.ErrBadConn
	}

	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}

	// Send command
	err = stmt.writeExecutePacket(ctx, dargs)
	if err != nil {
		return nil, stmt.mc.markBadConn(err)
	}

	mc := stmt.mc
	handleOk := stmt.mc.clearResult()

	// Read Result
	resLen, err := handleOk.readResultSetHeaderPacket(ctx)
	if err != nil {
		return nil, err
	}

	if resLen > 0 {
		// Columns
		if err = mc.readUntilEOF(ctx); err != nil {
			return nil, err
		}

		// Rows
		if err := mc.readUntilEOF(ctx); err != nil {
			return nil, err
		}
	}

	if err := handleOk.discardResults(ctx); err != nil {
		return nil, err
	}

	copied := mc.result
	return &copied, nil
}

func (mc *mysqlConn) CheckNamedValue(nv *driver.NamedValue) (err error) {
	nv.Value, err = converter{}.ConvertValue(nv.Value)
	return
}

// ResetSession implements driver.SessionResetter.
// (From Go 1.10)
func (mc *mysqlConn) ResetSession(ctx context.Context) error {
	if mc.closed.Load() {
		return driver.ErrBadConn
	}
	mc.reset = true
	return nil
}

// IsValid implements driver.Validator interface
// (From Go 1.15)
func (mc *mysqlConn) IsValid() bool {
	return !mc.closed.Load()
}

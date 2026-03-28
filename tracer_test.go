package mysql

import (
	"context"
	"database/sql/driver"
	"testing"
	"time"
)

// testTracer records trace calls for verification.
type testTracer struct {
	startCalled bool
	endCalled   bool
	query       string
	args        []driver.NamedValue
	err         error
	duration    time.Duration
	ctxKey      any
	ctxVal      any
}

type tracerCtxKey struct{}

func (t *testTracer) TraceQueryStart(ctx context.Context, query string, args []driver.NamedValue) context.Context {
	t.startCalled = true
	t.query = query
	t.args = args
	// Attach a value to context to verify it flows to TraceQueryEnd.
	return context.WithValue(ctx, tracerCtxKey{}, "traced")
}

func (t *testTracer) TraceQueryEnd(ctx context.Context, err error, duration time.Duration) {
	t.endCalled = true
	t.err = err
	t.duration = duration
	t.ctxVal = ctx.Value(tracerCtxKey{})
}

func (t *testTracer) reset() {
	t.startCalled = false
	t.endCalled = false
	t.query = ""
	t.args = nil
	t.err = nil
	t.duration = 0
	t.ctxVal = nil
}

func TestTraceQuery_WithTracer(t *testing.T) {
	tr := &testTracer{}
	mc := &mysqlConn{
		cfg: &Config{
			tracer: tr,
		},
	}

	args := []driver.NamedValue{
		{Ordinal: 1, Value: int64(42)},
		{Ordinal: 2, Value: "hello"},
	}

	ctx, finish := mc.traceQuery(context.Background(), "SELECT * FROM users WHERE id = ?", args)
	_ = ctx

	if !tr.startCalled {
		t.Fatal("TraceQueryStart was not called")
	}
	if tr.query != "SELECT * FROM users WHERE id = ?" {
		t.Fatalf("unexpected query: %q", tr.query)
	}
	if len(tr.args) != 2 {
		t.Fatalf("expected 2 args, got %d", len(tr.args))
	}
	if tr.args[0].Value != int64(42) {
		t.Fatalf("unexpected arg[0]: %v", tr.args[0].Value)
	}

	// Simulate some work
	time.Sleep(time.Millisecond)
	finish(nil)

	if !tr.endCalled {
		t.Fatal("TraceQueryEnd was not called")
	}
	if tr.err != nil {
		t.Fatalf("unexpected error: %v", tr.err)
	}
	if tr.duration < time.Millisecond {
		t.Fatalf("duration too short: %v", tr.duration)
	}
}

func TestTraceQuery_ContextFlows(t *testing.T) {
	tr := &testTracer{}
	mc := &mysqlConn{
		cfg: &Config{
			tracer: tr,
		},
	}

	_, finish := mc.traceQuery(context.Background(), "INSERT INTO t VALUES (?)", nil)
	finish(nil)

	// The context value set in TraceQueryStart should be visible in TraceQueryEnd.
	if tr.ctxVal != "traced" {
		t.Fatalf("context value not propagated: got %v, want %q", tr.ctxVal, "traced")
	}
}

func TestTraceQuery_WithError(t *testing.T) {
	tr := &testTracer{}
	mc := &mysqlConn{
		cfg: &Config{
			tracer: tr,
		},
	}

	_, finish := mc.traceQuery(context.Background(), "BAD SQL", nil)
	finish(ErrInvalidConn)

	if !tr.endCalled {
		t.Fatal("TraceQueryEnd was not called")
	}
	if tr.err != ErrInvalidConn {
		t.Fatalf("unexpected error: %v, want %v", tr.err, ErrInvalidConn)
	}
}

func TestTraceQuery_NilTracer(t *testing.T) {
	mc := &mysqlConn{
		cfg: &Config{
			tracer: nil,
		},
	}

	ctx := context.Background()
	retCtx, finish := mc.traceQuery(ctx, "SELECT 1", nil)

	// Context should be unchanged.
	if retCtx != ctx {
		t.Fatal("context should not be modified when tracer is nil")
	}

	// finish should be safe to call (no-op).
	finish(nil)
	finish(ErrInvalidConn)
}

func TestWithTracerOption(t *testing.T) {
	tr := &testTracer{}
	cfg := NewConfig()

	if cfg.tracer != nil {
		t.Fatal("tracer should be nil by default")
	}

	err := cfg.Apply(WithTracer(tr))
	if err != nil {
		t.Fatalf("Apply(WithTracer) failed: %v", err)
	}

	if cfg.tracer != tr {
		t.Fatal("tracer was not set")
	}
}

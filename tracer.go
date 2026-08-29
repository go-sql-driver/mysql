package mysql

import (
	"context"
	"database/sql/driver"
	"time"
)

// QueryTracer is an interface for tracing SQL query execution.
// It can be used for logging, metrics collection, or distributed tracing.
//
// TraceQueryStart is called before a query is executed. It receives the context,
// the SQL query string, and the named arguments. It returns a new context that
// will be passed to TraceQueryEnd — this allows attaching trace-specific metadata
// (e.g. span IDs) to the context.
//
// TraceQueryEnd is called after the query completes (or fails). It receives the
// context returned by TraceQueryStart, the error (nil on success), and the
// wall-clock duration of the query execution.
type QueryTracer interface {
	TraceQueryStart(ctx context.Context, query string, args []driver.NamedValue) context.Context
	TraceQueryEnd(ctx context.Context, err error, duration time.Duration)
}

// traceQuery starts tracing a query if a tracer is configured.
// It returns the (possibly updated) context and a finish function.
// The finish function must be called with the resulting error when the query completes.
// If no tracer is configured, the returned context is unchanged and the finish function is a no-op.
func (mc *mysqlConn) traceQuery(ctx context.Context, query string, args []driver.NamedValue) (context.Context, func(error)) {
	t := mc.cfg.tracer
	if t == nil {
		return ctx, func(error) {}
	}
	start := time.Now()
	ctx = t.TraceQueryStart(ctx, query, args)
	return ctx, func(err error) {
		t.TraceQueryEnd(ctx, err, time.Since(start))
	}
}

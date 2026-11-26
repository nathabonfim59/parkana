package catalog

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// TestPostgresRetryable checks only serialization failures and deadlocks
// rerun a transaction; a query canceled for an ended context, a unique
// violation or a plain error never do.
func TestPostgresRetryable(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{&pgconn.PgError{Code: pgSerializationFailure}, true},
		{&pgconn.PgError{Code: pgDeadlockDetected}, true},
		{wrapf(&pgconn.PgError{Code: pgDeadlockDetected}), true},
		{&pgconn.PgError{Code: "57014"}, false},
		{&pgconn.PgError{Code: pgUniqueViolation}, false},
		{context.Canceled, false},
		{errors.New("other"), false},
		{nil, false},
	} {
		if got := retryable(tc.err); got != tc.want {
			t.Errorf("retryable(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
}

// wrapf wraps err the way the catalog's write paths do.
func wrapf(err error) error { return errors.Join(errors.New("catalog: postgres: op"), err) }

// TestPostgresCtxError checks ctxError leaves errors alone while the
// context is live, and otherwise keeps the original error matchable while
// adding the context's.
func TestPostgresCtxError(t *testing.T) {
	live := t.Context()
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	pgErr := &pgconn.PgError{Code: "57014", Message: "canceling statement due to user request"}

	if err := ctxError(canceled, nil); err != nil {
		t.Fatalf("ctxError(canceled, nil) = %v, want nil", err)
	}
	if err := ctxError(live, pgErr); err != error(pgErr) {
		t.Fatalf("ctxError(live, err) = %v, want err unchanged", err)
	}
	err := ctxError(canceled, pgErr)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ctxError(canceled, pgErr) = %v, want errors.Is context.Canceled", err)
	}
	if got, ok := errors.AsType[*pgconn.PgError](err); !ok || got != pgErr {
		t.Fatalf("ctxError(canceled, pgErr) = %v, want the *pgconn.PgError still reachable", err)
	}
	if err := ctxError(canceled, context.Canceled); err != context.Canceled {
		t.Fatalf("ctxError(canceled, context.Canceled) = %v, want it unchanged", err)
	}
}

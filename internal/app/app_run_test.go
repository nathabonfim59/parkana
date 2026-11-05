package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
)

var errFatal = errors.New("manifest corrupt")

func TestIsCancellation(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"canceled", context.Canceled, true},
		{"wrapped canceled", fmt.Errorf("uploader: %w", context.Canceled), true},
		{"joined cancels", errors.Join(context.Canceled, fmt.Errorf("x: %w", context.Canceled)), true},
		{"joined fatal and cancel", errors.Join(errFatal, context.Canceled), false},
		{"wrapped joined fatal", fmt.Errorf("upload: %w", errors.Join(context.Canceled, errFatal)), false},
		{"deadline", context.DeadlineExceeded, false},
		{"other", errFatal, false},
	}
	for _, tt := range tests {
		if got := isCancellation(tt.err); got != tt.want {
			t.Errorf("%s: isCancellation(%v) = %v, want %v", tt.name, tt.err, got, tt.want)
		}
	}
}

func TestRunReportsJoinedFatal(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	services := map[string]Service{
		"clean": ServiceFunc(func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }),
		"fatal": ServiceFunc(func(ctx context.Context) error {
			cancel()
			return errors.Join(errFatal, context.Canceled)
		}),
	}
	err := Run(ctx, slog.New(slog.DiscardHandler), services)
	if !errors.Is(err, errFatal) {
		t.Fatalf("Run = %v, want the joined fatal error", err)
	}
}

func TestRunCleanShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	services := map[string]Service{
		"a": ServiceFunc(func(ctx context.Context) error { <-ctx.Done(); return fmt.Errorf("a: %w", ctx.Err()) }),
		"b": ServiceFunc(func(context.Context) error { return nil }),
	}
	if err := Run(ctx, slog.New(slog.DiscardHandler), services); err != nil {
		t.Fatalf("Run = %v, want nil on pure cancellation", err)
	}
}

func TestCombine(t *testing.T) {
	boom := errors.New("boom")
	waiter := func(Deps) (Service, error) {
		return ServiceFunc(func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }), nil
	}
	failer := func(Deps) (Service, error) {
		return ServiceFunc(func(context.Context) error { return boom }), nil
	}
	svc, err := Combine(waiter, failer)(Deps{})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Run(t.Context()); !errors.Is(err, boom) {
		t.Fatalf("Run = %v, want boom", err)
	}

	svc, err = Combine(waiter, waiter)(Deps{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := svc.Run(ctx); err != nil {
		t.Fatalf("Run after cancel = %v, want nil", err)
	}

	if _, err := Combine(waiter, func(Deps) (Service, error) { return nil, boom })(Deps{}); !errors.Is(err, boom) {
		t.Fatalf("factory error = %v, want boom", err)
	}
}

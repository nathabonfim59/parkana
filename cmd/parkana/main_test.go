package main

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

// startupLog is a concurrency-safe stderr for run that cancels the run once
// every wanted line has been logged, so the tests stop the process after it
// is up instead of after a fixed delay that a loaded machine can miss.
type startupLog struct {
	mu     sync.Mutex
	buf    bytes.Buffer
	want   []string
	cancel context.CancelFunc
}

func (l *startupLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	n, err := l.buf.Write(p)
	if l.cancel != nil {
		up := true
		for _, w := range l.want {
			up = up && strings.Contains(l.buf.String(), w)
		}
		if up {
			l.cancel()
			l.cancel = nil
		}
	}
	return n, err
}

func (l *startupLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// runUntilUp runs parkana with args until every line in want was logged,
// then cancels it and returns its full log and error. A run that never
// gets up fails after a generous deadline.
func runUntilUp(t *testing.T, args []string, want ...string) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	stderr := &startupLog{want: want, cancel: cancel}
	err := run(ctx, args, func(string) string { return "" }, stderr)
	if ctx.Err() == context.DeadlineExceeded {
		t.Fatalf("run did not log %q within 30s:\n%s", want, stderr.String())
	}
	return stderr.String(), err
}

func TestRunStartsAndStops(t *testing.T) {
	dir := t.TempDir()
	args := []string{"-http.addr=127.0.0.1:0", "-data.dir=" + dir}
	logs, err := runUntilUp(t, args, "http server listening", `msg="service starting"`)
	if err != nil {
		t.Fatalf("run: %v\n%s", err, logs)
	}
	for _, want := range []string{"starting parkana", "http server listening", "service stopped"} {
		if !strings.Contains(logs, want) {
			t.Errorf("log lacks %q:\n%s", want, logs)
		}
	}
}

func TestRunCompactorRole(t *testing.T) {
	dir := t.TempDir()
	args := []string{"-http.addr=127.0.0.1:0", "-data.dir=" + dir, "-roles=compactor"}
	logs, err := runUntilUp(t, args, "http server listening", `msg="service starting" service=compactor`)
	if err != nil {
		t.Fatalf("run: %v\n%s", err, logs)
	}
	for _, want := range []string{
		`msg="service starting" service=compactor`,
		`msg="service stopped" service=compactor`,
	} {
		if !strings.Contains(logs, want) {
			t.Errorf("log lacks %q:\n%s", want, logs)
		}
	}
	if strings.Contains(logs, "role not implemented yet") {
		t.Errorf("compactor role not wired:\n%s", logs)
	}
}

func TestRunHelpAndBadFlags(t *testing.T) {
	var stderr bytes.Buffer
	if err := run(t.Context(), []string{"-h"}, func(string) string { return "" }, &stderr); err != nil {
		t.Fatalf("-h: %v", err)
	}
	if err := run(t.Context(), []string{"-roles=router"}, func(string) string { return "" }, &stderr); err == nil {
		t.Fatal("bad role accepted")
	}
}

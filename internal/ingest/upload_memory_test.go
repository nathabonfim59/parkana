package ingest

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	uploadMemoryTestMiB int64 = 1 << 20
	uploadMemoryTestGiB int64 = 1 << 30
)

func TestResolveUploadMemoryBudget(t *testing.T) {
	finite := func(bytes int64) func() (int64, bool) {
		return func() (int64, bool) { return bytes, true }
	}
	unlimited := func() (int64, bool) { return 0, false }
	goLimit := func(bytes int64) func() int64 {
		return func() int64 { return bytes }
	}

	tests := []struct {
		name     string
		explicit int64
		limits   uploadMemoryLimits
		want     uploadMemoryBudget
		wantErr  bool
	}{
		{
			name:     "explicit wins over auto sources",
			explicit: 1234,
			limits: uploadMemoryLimits{
				Cgroup:        finite(8 * uploadMemoryTestGiB),
				GoMemoryLimit: goLimit(uploadMemoryTestGiB),
			},
			want: uploadMemoryBudget{Bytes: 1234, Source: uploadMemorySourceExplicit},
		},
		{
			name:     "negative explicit is rejected",
			explicit: -1,
			wantErr:  true,
		},
		{
			name:   "cgroup half",
			limits: uploadMemoryLimits{Cgroup: finite(2 * uploadMemoryTestGiB)},
			want:   uploadMemoryBudget{Bytes: uploadMemoryTestGiB, Source: uploadMemorySourceCgroup},
		},
		{
			name:   "gomemlimit half",
			limits: uploadMemoryLimits{GoMemoryLimit: goLimit(uploadMemoryTestGiB)},
			want:   uploadMemoryBudget{Bytes: 512 * uploadMemoryTestMiB, Source: uploadMemorySourceGoMemoryLimit},
		},
		{
			name: "smallest of cgroup and gomemlimit wins",
			limits: uploadMemoryLimits{
				Cgroup:        finite(4 * uploadMemoryTestGiB),
				GoMemoryLimit: goLimit(uploadMemoryTestGiB),
			},
			want: uploadMemoryBudget{Bytes: 512 * uploadMemoryTestMiB, Source: uploadMemorySourceGoMemoryLimit},
		},
		{
			name: "tie prefers cgroup",
			limits: uploadMemoryLimits{
				Cgroup:        finite(2 * uploadMemoryTestGiB),
				GoMemoryLimit: goLimit(2 * uploadMemoryTestGiB),
			},
			want: uploadMemoryBudget{Bytes: uploadMemoryTestGiB, Source: uploadMemorySourceCgroup},
		},
		{
			name: "no finite source falls back",
			want: uploadMemoryBudget{Bytes: uploadMemoryFallbackBudget, Source: uploadMemorySourceFallback},
		},
		{
			name: "unlimited and zero readings are ignored",
			limits: uploadMemoryLimits{
				Cgroup:        unlimited,
				GoMemoryLimit: goLimit(0),
			},
			want: uploadMemoryBudget{Bytes: uploadMemoryFallbackBudget, Source: uploadMemorySourceFallback},
		},
		{
			name:   "runtime unlimited is ignored",
			limits: uploadMemoryLimits{GoMemoryLimit: goLimit(1<<63 - 1)},
			want:   uploadMemoryBudget{Bytes: uploadMemoryFallbackBudget, Source: uploadMemorySourceFallback},
		},
		{
			name:   "negative gomemlimit is ignored",
			limits: uploadMemoryLimits{GoMemoryLimit: goLimit(-5)},
			want:   uploadMemoryBudget{Bytes: uploadMemoryFallbackBudget, Source: uploadMemorySourceFallback},
		},
		{
			name:   "sub-two-byte limit still admits one byte",
			limits: uploadMemoryLimits{Cgroup: finite(1)},
			want:   uploadMemoryBudget{Bytes: 1, Source: uploadMemorySourceCgroup},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveUploadMemoryBudget(tt.explicit, tt.limits)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("resolveUploadMemoryBudget(%d) = %+v, want error", tt.explicit, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveUploadMemoryBudget(%d): %v", tt.explicit, err)
			}
			if got != tt.want {
				t.Fatalf("resolveUploadMemoryBudget(%d) = %+v, want %+v", tt.explicit, got, tt.want)
			}
		})
	}
}

func TestReadCgroupMemoryLimit(t *testing.T) {
	tests := []struct {
		name        string
		proc        string
		noProc      bool
		mountinfo   string
		noMountinfo bool
		files       map[string]string
		want        int64
		ok          bool
	}{
		{
			name:      "v2 walks ancestors and takes the smallest",
			proc:      "0::/a/b\n",
			mountinfo: `30 23 0:26 / {{dir}}/sys/fs/cgroup rw,nosuid,nodev,noexec,relatime - cgroup2 cgroup2 rw,nsdelegate` + "\n",
			files: map[string]string{
				"sys/fs/cgroup/memory.max":     "max",
				"sys/fs/cgroup/a/memory.max":   strconv.FormatInt(4*uploadMemoryTestGiB, 10),
				"sys/fs/cgroup/a/b/memory.max": strconv.FormatInt(2*uploadMemoryTestGiB, 10),
			},
			want: 2 * uploadMemoryTestGiB,
			ok:   true,
		},
		{
			name:      "v2 unlimited everywhere",
			proc:      "0::/\n",
			mountinfo: `30 23 0:26 / {{dir}}/sys/fs/cgroup rw - cgroup2 cgroup2 rw` + "\n",
			files: map[string]string{
				"sys/fs/cgroup/memory.max": "max",
			},
		},
		{
			name:      "v1 memory controller list and ancestors",
			proc:      "7:cpu,memory:/a/b\n",
			mountinfo: `40 23 0:25 / {{dir}}/sys/fs/cgroup/memory rw - cgroup cgroup rw,memory` + "\n",
			files: map[string]string{
				"sys/fs/cgroup/memory/memory.limit_in_bytes":     strconv.FormatInt(uploadMemoryCgroupV1UnlimitedFloor, 10),
				"sys/fs/cgroup/memory/a/memory.limit_in_bytes":   strconv.FormatInt(3*uploadMemoryTestGiB, 10),
				"sys/fs/cgroup/memory/a/b/memory.limit_in_bytes": strconv.FormatInt(6*uploadMemoryTestGiB, 10),
			},
			want: 3 * uploadMemoryTestGiB,
			ok:   true,
		},
		{
			name:      "v1 4 KiB unlimited sentinel",
			proc:      "7:memory:/a\n",
			mountinfo: `40 23 0:25 / {{dir}}/sys/fs/cgroup/memory rw - cgroup cgroup rw,memory` + "\n",
			files: map[string]string{
				"sys/fs/cgroup/memory/memory.limit_in_bytes":   strconv.FormatInt(math.MaxInt64-4095, 10),
				"sys/fs/cgroup/memory/a/memory.limit_in_bytes": strconv.FormatInt(math.MaxInt64-4095, 10),
			},
		},
		{
			name:      "v1 64 KiB unlimited sentinel",
			proc:      "7:memory:/a\n",
			mountinfo: `40 23 0:25 / {{dir}}/sys/fs/cgroup/memory rw - cgroup cgroup rw,memory` + "\n",
			files: map[string]string{
				"sys/fs/cgroup/memory/memory.limit_in_bytes":   strconv.FormatInt(math.MaxInt64-65535, 10),
				"sys/fs/cgroup/memory/a/memory.limit_in_bytes": strconv.FormatInt(math.MaxInt64-65535, 10),
			},
		},
		{
			name: "v2 and v1 take the smallest",
			proc: "0::/\n7:memory:/\n",
			mountinfo: `30 23 0:26 / {{dir}}/sys/fs/cgroup rw - cgroup2 cgroup2 rw` + "\n" +
				`40 23 0:25 / {{dir}}/sys/fs/cgroup/memory rw - cgroup cgroup rw,memory` + "\n",
			files: map[string]string{
				"sys/fs/cgroup/memory.max":                   strconv.FormatInt(4*uploadMemoryTestGiB, 10),
				"sys/fs/cgroup/memory/memory.limit_in_bytes": strconv.FormatInt(2*uploadMemoryTestGiB, 10),
			},
			want: 2 * uploadMemoryTestGiB,
			ok:   true,
		},
		{
			name:      "delegated v2 subtree strips a non-root mount root",
			proc:      "0::/user.slice/user-1000.slice/session-1.scope\n",
			mountinfo: `31 30 0:26 /user.slice/user-1000.slice {{dir}}/run/cgroup-delegated rw - cgroup2 cgroup2 rw` + "\n",
			files: map[string]string{
				"run/cgroup-delegated/memory.max":                 strconv.FormatInt(4*uploadMemoryTestGiB, 10),
				"run/cgroup-delegated/session-1.scope/memory.max": strconv.FormatInt(2*uploadMemoryTestGiB, 10),
			},
			want: 2 * uploadMemoryTestGiB,
			ok:   true,
		},
		{
			name:      "escaped mountpoint is unescaped",
			proc:      "0::/a\n",
			mountinfo: `32 30 0:26 / {{dir}}/run/cgroup\040delegated rw - cgroup2 cgroup2 rw` + "\n",
			files: map[string]string{
				"run/cgroup delegated/memory.max": strconv.FormatInt(uploadMemoryTestGiB, 10),
			},
			want: uploadMemoryTestGiB,
			ok:   true,
		},
		{
			name:      "membership outside the mount root is skipped",
			proc:      "0::/other\n",
			mountinfo: `31 30 0:26 /user.slice {{dir}}/run/cgroup rw - cgroup2 cgroup2 rw` + "\n",
			files: map[string]string{
				"run/cgroup/memory.max": strconv.FormatInt(uploadMemoryTestGiB, 10),
			},
		},
		{
			name:      "membership equals a delegated mount root",
			proc:      "0::/user.slice/x\n",
			mountinfo: `31 30 0:26 /user.slice/x {{dir}}/run/cgroup rw - cgroup2 cgroup2 rw` + "\n",
			files: map[string]string{
				"run/cgroup/memory.max": strconv.FormatInt(2*uploadMemoryTestGiB, 10),
			},
			want: 2 * uploadMemoryTestGiB,
			ok:   true,
		},
		{
			name:      "escaped mount root is unescaped",
			proc:      "0::/user slice/x\n",
			mountinfo: `41 40 0:26 /user\040slice {{dir}}/run/cgroup rw - cgroup2 cgroup2 rw` + "\n",
			files: map[string]string{
				"run/cgroup/memory.max":   strconv.FormatInt(4*uploadMemoryTestGiB, 10),
				"run/cgroup/x/memory.max": strconv.FormatInt(2*uploadMemoryTestGiB, 10),
			},
			want: 2 * uploadMemoryTestGiB,
			ok:   true,
		},
		{
			name:      "v1 delegated subtree",
			proc:      "7:memory:/user.slice/x\n",
			mountinfo: `41 40 0:25 /user.slice {{dir}}/run/memory rw - cgroup cgroup rw,memory` + "\n",
			files: map[string]string{
				"run/memory/memory.limit_in_bytes":   strconv.FormatInt(uploadMemoryCgroupV1UnlimitedFloor, 10),
				"run/memory/x/memory.limit_in_bytes": strconv.FormatInt(3*uploadMemoryTestGiB, 10),
			},
			want: 3 * uploadMemoryTestGiB,
			ok:   true,
		},
		{
			name:      "v1 mount without the memory controller is ignored",
			proc:      "7:memory:/a\n",
			mountinfo: `42 40 0:25 / {{dir}}/run/cgroup-cpu rw - cgroup cgroup rw,cpu` + "\n",
			files: map[string]string{
				"run/cgroup-cpu/a/memory.limit_in_bytes": strconv.FormatInt(uploadMemoryTestGiB, 10),
			},
		},
		{
			name:      "malformed mountinfo line is skipped",
			proc:      "0::/a\n",
			mountinfo: "garbage\n" + `30 23 0:26 / {{dir}}/sys/fs/cgroup rw - cgroup2 cgroup2 rw` + "\n",
			files: map[string]string{
				"sys/fs/cgroup/a/memory.max": strconv.FormatInt(uploadMemoryTestGiB, 10),
			},
			want: uploadMemoryTestGiB,
			ok:   true,
		},
		{
			name:      "malformed level is skipped",
			proc:      "0::/a\n",
			mountinfo: `30 23 0:26 / {{dir}}/sys/fs/cgroup rw - cgroup2 cgroup2 rw` + "\n",
			files: map[string]string{
				"sys/fs/cgroup/a/memory.max": "not-a-number",
				"sys/fs/cgroup/memory.max":   strconv.FormatInt(3*uploadMemoryTestGiB, 10),
			},
			want: 3 * uploadMemoryTestGiB,
			ok:   true,
		},
		{
			name:        "missing mountinfo means no cgroup limit",
			proc:        "0::/\n",
			noMountinfo: true,
			files: map[string]string{
				"sys/fs/cgroup/memory.max": strconv.FormatInt(uploadMemoryTestGiB, 10),
			},
		},
		{
			name:      "missing proc means no cgroup limit",
			noProc:    true,
			mountinfo: `30 23 0:26 / {{dir}}/sys/fs/cgroup rw - cgroup2 cgroup2 rw` + "\n",
			files: map[string]string{
				"sys/fs/cgroup/memory.max": strconv.FormatInt(uploadMemoryTestGiB, 10),
			},
		},
		{
			name:      "empty proc means no cgroup limit",
			proc:      "",
			mountinfo: `30 23 0:26 / {{dir}}/sys/fs/cgroup rw - cgroup2 cgroup2 rw` + "\n",
			files: map[string]string{
				"sys/fs/cgroup/memory.max": strconv.FormatInt(uploadMemoryTestGiB, 10),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			for name, content := range tt.files {
				writeUploadMemoryTestFile(t, filepath.Join(dir, filepath.FromSlash(name)), content)
			}
			proc := filepath.Join(dir, "cgroup")
			if !tt.noProc {
				writeUploadMemoryTestFile(t, proc, tt.proc)
			}
			mountinfo := filepath.Join(dir, "mountinfo")
			if !tt.noMountinfo {
				writeUploadMemoryTestFile(t, mountinfo, strings.ReplaceAll(tt.mountinfo, "{{dir}}", dir))
			}
			got, ok := readCgroupMemoryLimit(proc, mountinfo, os.ReadFile)
			if ok != tt.ok || got != tt.want {
				t.Fatalf("readCgroupMemoryLimit = (%d, %t), want (%d, %t)", got, ok, tt.want, tt.ok)
			}
		})
	}
}

func TestReadCgroupMemoryLimitNilReader(t *testing.T) {
	if got, ok := readCgroupMemoryLimit("/proc/self/cgroup", "/proc/self/mountinfo", nil); ok || got != 0 {
		t.Fatalf("readCgroupMemoryLimit with a nil reader = (%d, %t), want (0, false)", got, ok)
	}
}

// TestNewUploadMemoryAdmissionForHost exercises the real host wrapper instead
// of injected fixtures. It asserts only host-independent invariants because
// the cgroup hierarchy and GOMEMLIMIT differ between machines and CI.
func TestNewUploadMemoryAdmissionForHost(t *testing.T) {
	limitBefore := debug.SetMemoryLimit(-1)
	admission, err := newUploadMemoryAdmissionForHost(0, uploadMemoryHooks{})
	if err != nil {
		t.Fatalf("newUploadMemoryAdmissionForHost(0): %v", err)
	}
	budget := admission.Budget()
	if budget.Bytes <= 0 {
		t.Fatalf("host budget = %d, want positive", budget.Bytes)
	}
	switch budget.Source {
	case uploadMemorySourceCgroup, uploadMemorySourceGoMemoryLimit, uploadMemorySourceFallback:
	default:
		t.Fatalf("host budget source = %v, want an automatic source", budget.Source)
	}
	if got := debug.SetMemoryLimit(-1); got != limitBefore {
		t.Fatalf("host resolver changed GOMEMLIMIT: before=%d after=%d", limitBefore, got)
	}
	release, err := admission.Acquire(t.Context(), budget.Bytes)
	if err != nil {
		t.Fatalf("acquire full host budget %d: %v", budget.Bytes, err)
	}
	release()
}

func TestUploadMemorySourceString(t *testing.T) {
	tests := map[uploadMemorySource]string{
		uploadMemorySourceExplicit:      "explicit",
		uploadMemorySourceCgroup:        "cgroup",
		uploadMemorySourceGoMemoryLimit: "gomemlimit",
		uploadMemorySourceFallback:      "fallback",
		uploadMemorySource(0):           "unknown",
	}
	for source, want := range tests {
		if got := source.String(); got != want {
			t.Fatalf("uploadMemorySource(%d).String() = %q, want %q", source, got, want)
		}
	}
}

func TestUploadMemoryAdmissionRejectsInvalidConfiguration(t *testing.T) {
	for _, budget := range []int64{0, -1} {
		if _, err := newUploadMemoryAdmission(uploadMemoryBudget{Bytes: budget}, uploadMemoryHooks{}); err == nil {
			t.Fatalf("newUploadMemoryAdmission(%d) succeeded, want error", budget)
		}
	}
	var nilAdmission *uploadMemoryAdmission
	if _, err := nilAdmission.Acquire(t.Context(), 1); err == nil {
		t.Fatal("nil admission Acquire succeeded, want error")
	}
}

func TestNewUploadMemoryAdmissionForHostExplicit(t *testing.T) {
	admission, err := newUploadMemoryAdmissionForHost(4096, uploadMemoryHooks{})
	if err != nil {
		t.Fatalf("newUploadMemoryAdmissionForHost: %v", err)
	}
	if got, want := admission.Budget(), (uploadMemoryBudget{Bytes: 4096, Source: uploadMemorySourceExplicit}); got != want {
		t.Fatalf("Budget() = %+v, want %+v", got, want)
	}
	release, err := admission.Acquire(t.Context(), 4096)
	if err != nil {
		t.Fatalf("acquire explicit budget: %v", err)
	}
	release()
}

func TestUploadMemoryAdmissionWeightedOverlap(t *testing.T) {
	admission := newUploadMemoryTestAdmission(t, 100, uploadMemoryHooks{})
	releaseA, err := admission.Acquire(t.Context(), 30)
	if err != nil {
		t.Fatalf("acquire 30: %v", err)
	}
	releaseB, err := admission.Acquire(t.Context(), 60)
	if err != nil {
		releaseA()
		t.Fatalf("acquire 60: %v", err)
	}

	waiting := make(chan uploadMemoryTestResult, 1)
	go func() {
		release, err := admission.Acquire(t.Context(), 50)
		waiting <- uploadMemoryTestResult{release: release, err: err}
	}()
	requireUploadMemoryBlocked(t, waiting, "50 bytes with 10 free")

	releaseA()
	requireUploadMemoryBlocked(t, waiting, "50 bytes with 40 free after one release")

	releaseB()
	got := requireUploadMemoryAdmitted(t, waiting)
	got.release()
}

func TestUploadMemoryAdmissionCancellation(t *testing.T) {
	var acquired, released atomic.Int64
	admission := newUploadMemoryTestAdmission(t, 100, uploadMemoryHooks{
		Acquire: func(uploadMemoryLease) { acquired.Add(1) },
		Release: func(uploadMemoryLease) { released.Add(1) },
	})
	hold, err := admission.Acquire(t.Context(), 100)
	if err != nil {
		t.Fatalf("hold budget: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	waiting := make(chan uploadMemoryTestResult, 1)
	go func() {
		release, err := admission.Acquire(ctx, 1)
		waiting <- uploadMemoryTestResult{release: release, err: err}
	}()
	requireUploadMemoryBlocked(t, waiting, "1 byte with the budget held")

	cancel()
	got := requireUploadMemoryError(t, waiting)
	if !errors.Is(got, context.Canceled) {
		t.Fatalf("waiting Acquire error = %v, want context.Canceled", got)
	}
	if acquired.Load() != 1 || released.Load() != 0 {
		t.Fatalf("callbacks after cancellation: acquired=%d released=%d, want 1/0", acquired.Load(), released.Load())
	}

	hold()
	release, err := admission.Acquire(t.Context(), 100)
	if err != nil {
		t.Fatalf("acquire after canceled waiter: %v", err)
	}
	release()
	if acquired.Load() != 2 || released.Load() != 2 {
		t.Fatalf("callbacks = acquired %d released %d, want 2/2", acquired.Load(), released.Load())
	}
}

func TestUploadMemoryAdmissionAlreadyCanceledNeverAcquires(t *testing.T) {
	var acquired atomic.Int64
	admission := newUploadMemoryTestAdmission(t, 100, uploadMemoryHooks{
		Acquire: func(uploadMemoryLease) { acquired.Add(1) },
	})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	release, err := admission.Acquire(ctx, 1)
	if release != nil {
		t.Fatal("already-canceled Acquire returned a release function")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("already-canceled Acquire error = %v, want context.Canceled", err)
	}
	if acquired.Load() != 0 {
		t.Fatalf("already-canceled Acquire admitted %d jobs", acquired.Load())
	}
	// The free budget proves nothing was charged.
	release, err = admission.Acquire(t.Context(), 100)
	if err != nil {
		t.Fatalf("acquire full budget after canceled call: %v", err)
	}
	release()
}

func TestUploadMemoryAdmissionOversizedWaitsAlone(t *testing.T) {
	var warnings, oversized atomic.Int64
	admitted := make(chan uploadMemoryLease, 1)
	admission := newUploadMemoryTestAdmission(t, 100, uploadMemoryHooks{
		Warning: func(uploadMemoryWarning) { warnings.Add(1) },
		Oversized: func(uploadMemoryWarning) {
			oversized.Add(1)
		},
		Acquire: func(lease uploadMemoryLease) {
			if lease.Clamped {
				admitted <- lease
			}
		},
	})
	hold, err := admission.Acquire(t.Context(), 100)
	if err != nil {
		t.Fatalf("hold budget: %v", err)
	}

	waiting := make(chan uploadMemoryTestResult, 1)
	go func() {
		release, err := admission.Acquire(t.Context(), 250)
		waiting <- uploadMemoryTestResult{release: release, err: err}
	}()
	requireUploadMemoryBlocked(t, waiting, "oversized job while the budget is held")
	if warnings.Load() != 0 || oversized.Load() != 0 {
		t.Fatalf("oversized reported before admission: warnings=%d oversized=%d", warnings.Load(), oversized.Load())
	}

	hold()
	var lease uploadMemoryLease
	select {
	case lease = <-admitted:
	case <-time.After(5 * time.Second):
		t.Fatal("oversized job was never admitted")
	}
	if !lease.Clamped || lease.Weight != 100 || lease.Budget != 100 || lease.Estimate != 250 {
		t.Fatalf("oversized lease = %+v, want clamped weight 100 of budget 100 for estimate 250", lease)
	}
	if warnings.Load() != 1 || oversized.Load() != 1 {
		t.Fatalf("oversized callbacks = warnings %d oversized %d, want 1/1", warnings.Load(), oversized.Load())
	}

	// The oversized job holds the full budget, so even one byte must wait.
	small := make(chan uploadMemoryTestResult, 1)
	go func() {
		release, err := admission.Acquire(t.Context(), 1)
		small <- uploadMemoryTestResult{release: release, err: err}
	}()
	requireUploadMemoryBlocked(t, small, "small job while the oversized job runs alone")

	oversizedRelease := requireUploadMemoryAdmitted(t, waiting)
	oversizedRelease.release()
	requireUploadMemoryAdmitted(t, small).release()
}

func TestUploadMemoryAdmissionCanceledOversizedIsNotCounted(t *testing.T) {
	var warnings, oversized atomic.Int64
	admission := newUploadMemoryTestAdmission(t, 100, uploadMemoryHooks{
		Warning:   func(uploadMemoryWarning) { warnings.Add(1) },
		Oversized: func(uploadMemoryWarning) { oversized.Add(1) },
	})
	hold, err := admission.Acquire(t.Context(), 100)
	if err != nil {
		t.Fatalf("hold budget: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	waiting := make(chan uploadMemoryTestResult, 1)
	go func() {
		release, err := admission.Acquire(ctx, 250)
		waiting <- uploadMemoryTestResult{release: release, err: err}
	}()
	requireUploadMemoryBlocked(t, waiting, "oversized job while the budget is held")
	cancel()
	if err := requireUploadMemoryError(t, waiting); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled oversized Acquire error = %v, want context.Canceled", err)
	}
	if warnings.Load() != 0 || oversized.Load() != 0 {
		t.Fatalf("canceled oversized job was reported: warnings=%d oversized=%d", warnings.Load(), oversized.Load())
	}
	hold()
}

func TestUploadMemoryAdmissionUnknownEstimateTakesFullBudget(t *testing.T) {
	admitted := make(chan uploadMemoryLease, 1)
	admission := newUploadMemoryTestAdmission(t, 100, uploadMemoryHooks{
		Acquire: func(lease uploadMemoryLease) {
			if lease.Unknown {
				admitted <- lease
			}
		},
	})
	hold, err := admission.Acquire(t.Context(), 1)
	if err != nil {
		t.Fatalf("hold one byte: %v", err)
	}

	waiting := make(chan uploadMemoryTestResult, 1)
	go func() {
		release, err := admission.Acquire(t.Context(), 0)
		waiting <- uploadMemoryTestResult{release: release, err: err}
	}()
	requireUploadMemoryBlocked(t, waiting, "unknown estimate with one byte held")

	hold()
	var lease uploadMemoryLease
	select {
	case lease = <-admitted:
	case <-time.After(5 * time.Second):
		t.Fatal("unknown-estimate job was never admitted")
	}
	if !lease.Unknown || lease.Clamped || lease.Weight != 100 || lease.Estimate != 0 {
		t.Fatalf("unknown lease = %+v, want unknown weight 100 of budget 100", lease)
	}
	requireUploadMemoryAdmitted(t, waiting).release()
}

func TestUploadMemoryAdmissionReleaseIsIdempotent(t *testing.T) {
	var released atomic.Int64
	admission := newUploadMemoryTestAdmission(t, 100, uploadMemoryHooks{
		Release: func(uploadMemoryLease) { released.Add(1) },
	})
	release, err := admission.Acquire(t.Context(), 100)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	release()
	release()
	if released.Load() != 1 {
		t.Fatalf("release callback ran %d times, want 1", released.Load())
	}
	again, err := admission.Acquire(t.Context(), 100)
	if err != nil {
		t.Fatalf("acquire after double release: %v", err)
	}
	again()
}

func TestUploadMemoryAdmissionReleaseHookPrecedesReuse(t *testing.T) {
	var releaseStarted atomic.Bool
	releaseEntered := make(chan struct{})
	gate := make(chan struct{})
	nextAcquired := make(chan uploadMemoryLease, 1)
	admission := newUploadMemoryTestAdmission(t, 100, uploadMemoryHooks{
		Release: func(uploadMemoryLease) {
			if releaseStarted.CompareAndSwap(false, true) {
				close(releaseEntered)
				<-gate
			}
		},
		Acquire: func(lease uploadMemoryLease) {
			if releaseStarted.Load() {
				nextAcquired <- lease
			}
		},
	})
	hold, err := admission.Acquire(t.Context(), 100)
	if err != nil {
		t.Fatalf("hold budget: %v", err)
	}

	waiting := make(chan uploadMemoryTestResult, 1)
	go func() {
		release, err := admission.Acquire(t.Context(), 100)
		waiting <- uploadMemoryTestResult{release: release, err: err}
	}()
	requireUploadMemoryBlocked(t, waiting, "waiter while the budget is held")

	released := make(chan struct{})
	go func() {
		hold()
		close(released)
	}()
	select {
	case <-releaseEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("release hook never ran")
	}

	// The release hook is still blocked, so its weight must not be visible to
	// the next waiter yet.
	requireUploadMemoryBlocked(t, waiting, "waiter while the release hook is held")
	select {
	case lease := <-nextAcquired:
		t.Fatalf("next job was admitted before release bookkeeping finished: %+v", lease)
	case <-time.After(25 * time.Millisecond):
	}

	close(gate)
	select {
	case <-released:
	case <-time.After(5 * time.Second):
		t.Fatal("release never returned")
	}
	requireUploadMemoryAdmitted(t, waiting).release()
}

func TestUploadMemoryAdmissionAcquireHookPanicReleases(t *testing.T) {
	var panicOnce atomic.Bool
	panicOnce.Store(true)
	admission := newUploadMemoryTestAdmission(t, 100, uploadMemoryHooks{
		Acquire: func(uploadMemoryLease) {
			if panicOnce.CompareAndSwap(true, false) {
				panic("hook boom")
			}
		},
	})
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("Acquire did not re-raise the hook panic")
			}
		}()
		if _, err := admission.Acquire(t.Context(), 100); err != nil {
			t.Fatalf("Acquire returned an error instead of panicking: %v", err)
		}
	}()
	release, err := admission.Acquire(t.Context(), 100)
	if err != nil {
		t.Fatalf("budget leaked after a panicking hook: %v", err)
	}
	release()
}

func TestUploadMemoryAdmissionReleaseHookPanicReleases(t *testing.T) {
	var (
		panicOnce atomic.Bool
		released  atomic.Int64
	)
	panicOnce.Store(true)
	admission := newUploadMemoryTestAdmission(t, 100, uploadMemoryHooks{
		Release: func(uploadMemoryLease) {
			released.Add(1)
			if panicOnce.CompareAndSwap(true, false) {
				panic("release hook boom")
			}
		},
	})
	release, err := admission.Acquire(t.Context(), 100)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("release did not re-raise the hook panic")
			}
		}()
		release()
	}()
	if released.Load() != 1 {
		t.Fatalf("release hook ran %d times, want 1", released.Load())
	}
	// The panic must have returned the weight, and the once-only release must
	// not run the bookkeeping again.
	release()
	if released.Load() != 1 {
		t.Fatalf("repeated release after a panicking hook ran the callback again: %d", released.Load())
	}
	again, err := admission.Acquire(t.Context(), 100)
	if err != nil {
		t.Fatalf("budget leaked after a panicking release hook: %v", err)
	}
	again()
	if released.Load() != 2 {
		t.Fatalf("release hook ran %d times after reused budget, want 2", released.Load())
	}
}

func TestUploadMemoryAdmissionConcurrentCap(t *testing.T) {
	var (
		inUse  atomic.Int64
		over   atomic.Bool
		failed atomic.Int64
		wg     sync.WaitGroup
	)
	admission := newUploadMemoryTestAdmission(t, 100, uploadMemoryHooks{
		Acquire: func(lease uploadMemoryLease) {
			if inUse.Add(lease.Weight) > 100 {
				over.Store(true)
			}
		},
		Release: func(lease uploadMemoryLease) { inUse.Add(-lease.Weight) },
	})
	for i := range 200 {
		wg.Go(func() {
			release, err := admission.Acquire(t.Context(), int64(1+i%30))
			if err != nil {
				failed.Add(1)
				return
			}
			defer release()
		})
	}
	wg.Wait()
	if failed.Load() != 0 {
		t.Fatalf("%d concurrent acquisitions failed", failed.Load())
	}
	if over.Load() {
		t.Fatal("in-flight weight exceeded the budget")
	}
	if inUse.Load() != 0 {
		t.Fatalf("in-flight weight after all releases = %d, want 0", inUse.Load())
	}
}

func TestUploadMemoryAdmissionBudget(t *testing.T) {
	admission, err := newUploadMemoryAdmission(
		uploadMemoryBudget{Bytes: 4096, Source: uploadMemorySourceCgroup},
		uploadMemoryHooks{},
	)
	if err != nil {
		t.Fatalf("newUploadMemoryAdmission: %v", err)
	}
	if got, want := admission.Budget(), (uploadMemoryBudget{Bytes: 4096, Source: uploadMemorySourceCgroup}); got != want {
		t.Fatalf("Budget() = %+v, want %+v", got, want)
	}
	var nilAdmission *uploadMemoryAdmission
	if got := nilAdmission.Budget(); got != (uploadMemoryBudget{}) {
		t.Fatalf("nil Budget() = %+v, want zero", got)
	}
}

// uploadMemoryTestResult is one Acquire outcome delivered from a test
// goroutine.
type uploadMemoryTestResult struct {
	release func()
	err     error
}

func newUploadMemoryTestAdmission(t *testing.T, budget int64, hooks uploadMemoryHooks) *uploadMemoryAdmission {
	t.Helper()
	admission, err := newUploadMemoryAdmission(
		uploadMemoryBudget{Bytes: budget, Source: uploadMemorySourceExplicit},
		hooks,
	)
	if err != nil {
		t.Fatalf("newUploadMemoryAdmission(%d): %v", budget, err)
	}
	return admission
}

// requireUploadMemoryBlocked fails if ch delivers an outcome within the wait.
func requireUploadMemoryBlocked(t *testing.T, ch <-chan uploadMemoryTestResult, what string) {
	t.Helper()
	select {
	case got := <-ch:
		if got.err != nil {
			t.Fatalf("%s: waiting Acquire failed: %v", what, got.err)
		}
		got.release()
		t.Fatalf("%s: Acquire was admitted while it had to wait", what)
	case <-time.After(25 * time.Millisecond):
	}
}

// requireUploadMemoryAdmitted waits for an admitted outcome and fails on an
// error or timeout.
func requireUploadMemoryAdmitted(t *testing.T, ch <-chan uploadMemoryTestResult) uploadMemoryTestResult {
	t.Helper()
	select {
	case got := <-ch:
		if got.err != nil {
			t.Fatalf("waiting Acquire failed: %v", got.err)
		}
		return got
	case <-time.After(5 * time.Second):
		t.Fatal("waiting Acquire was never admitted")
		return uploadMemoryTestResult{}
	}
}

// requireUploadMemoryError waits for a failed outcome and returns its error.
func requireUploadMemoryError(t *testing.T, ch <-chan uploadMemoryTestResult) error {
	t.Helper()
	select {
	case got := <-ch:
		if got.err == nil {
			got.release()
			t.Fatal("waiting Acquire was admitted, want an error")
		}
		return got.err
	case <-time.After(5 * time.Second):
		t.Fatal("waiting Acquire never returned")
		return nil
	}
}

func writeUploadMemoryTestFile(t *testing.T, name, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(name), err)
	}
	if err := os.WriteFile(name, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
}

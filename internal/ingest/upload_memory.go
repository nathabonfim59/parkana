package ingest

// Upload memory admission. One upload receipt merges Parquet sources and
// builds a search sidecar, so its peak resident memory grows with the source
// fan-in, the decoder's batches and the sidecar maps, not only with the
// compressed source bytes. This file owns a private weighted byte budget that
// bounds how much estimated merge footprint may be admitted at once, and the
// resolution of that budget from an explicit setting, the effective cgroup
// hierarchy or the runtime's GOMEMLIMIT.
//
// The estimator lives elsewhere: callers pass a positive byte estimate. A
// non-positive estimate is charged the full budget, so an unknown job cannot
// over-subscribe the admission budget; actual memory can still exceed its
// estimate, because estimates plan resources rather than cap RSS. A job whose
// estimate exceeds the budget is clamped to the full budget and therefore
// waits until every other admitted job released, then runs alone; it is never
// rejected, because a permanent resource error would strand a durable upload
// receipt.

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/sync/semaphore"
)

const (
	// uploadMemoryFallbackBudget is the conservative budget used when neither
	// the cgroup hierarchy nor GOMEMLIMIT reports a finite limit.
	uploadMemoryFallbackBudget int64 = 512 << 20
	// uploadMemoryCgroupV1UnlimitedFloor is the smaller of the canonical
	// values cgroup v1 writes for an unlimited memory controller:
	// 0x7FFFFFFFFFFFF000 on a 4 KiB page system and 0x7FFFFFFFFFFF0000 on a
	// 64 KiB page system. Anything at or above it means "no limit": the
	// remaining 64 KiB up to MaxInt64 is far beyond any physical memory.
	uploadMemoryCgroupV1UnlimitedFloor int64 = math.MaxInt64 - 65535
)

// uploadMemorySource names where a resolved admission budget came from.
type uploadMemorySource uint8

const (
	// uploadMemorySourceExplicit means a positive configured byte budget.
	uploadMemorySourceExplicit uploadMemorySource = iota + 1
	// uploadMemorySourceCgroup means half the smallest finite effective
	// cgroup v2/v1 hierarchy limit.
	uploadMemorySourceCgroup
	// uploadMemorySourceGoMemoryLimit means half the runtime GOMEMLIMIT.
	uploadMemorySourceGoMemoryLimit
	// uploadMemorySourceFallback means neither limit was known and the
	// conservative uploadMemoryFallbackBudget applies.
	uploadMemorySourceFallback
)

// String returns a stable name for logs and metrics.
func (s uploadMemorySource) String() string {
	switch s {
	case uploadMemorySourceExplicit:
		return "explicit"
	case uploadMemorySourceCgroup:
		return "cgroup"
	case uploadMemorySourceGoMemoryLimit:
		return "gomemlimit"
	case uploadMemorySourceFallback:
		return "fallback"
	default:
		return "unknown"
	}
}

// uploadMemoryBudget is a resolved admission budget and its provenance.
type uploadMemoryBudget struct {
	// Bytes is the positive byte budget admission charges against.
	Bytes int64
	// Source records how Bytes was chosen.
	Source uploadMemorySource
}

// uploadMemoryLease describes one admitted upload job.
type uploadMemoryLease struct {
	// Estimate is the caller's estimate in bytes; it may be non-positive.
	Estimate int64
	// Weight is the bytes charged to the budget.
	Weight int64
	// Budget is the admission's resolved byte budget.
	Budget int64
	// Unknown reports that the estimate was not positive, so the job was
	// charged the full budget conservatively.
	Unknown bool
	// Clamped reports that the estimate exceeded the budget, so the weight was
	// clamped to the full budget and the job waits alone.
	Clamped bool
}

// uploadMemoryWarning is the structured diagnostic emitted for an oversized
// estimate. The integration layer turns it into a structured log line and the
// oversized counter.
type uploadMemoryWarning struct {
	Estimate int64
	Budget   int64
	Weight   int64
}

// uploadMemoryHooks are the optional admission callbacks. They run on the
// acquiring or releasing goroutine and must be fast and non-blocking; the
// Acquire hook runs after the weight was admitted and before the job starts,
// and the Release hook runs before the weight becomes available to another
// waiter. They must not panic: a panicking Acquire or Release hook cannot
// strand the weight, and the panic is re-raised after the weight is returned.
type uploadMemoryHooks struct {
	// Warning observes an oversized job that was admitted after waiting. A
	// request canceled while waiting is never reported, but one canceled after
	// admission is still observed.
	Warning func(uploadMemoryWarning)
	// Oversized counts an oversized job that was admitted after waiting, for
	// parkana_upload_oversized_jobs_total. It counts admitted attempts, not
	// guaranteed runs: a job canceled after admission is still counted.
	Oversized func(uploadMemoryWarning)
	// Acquire observes an admitted job, for the active-jobs gauge.
	Acquire func(uploadMemoryLease)
	// Release observes a returned job, for the active-jobs gauge.
	Release func(uploadMemoryLease)
}

// uploadMemoryClassify returns how one estimate is charged. It is the single
// source of truth shared by Acquire and the admission log: a non-positive
// estimate is unknown and charges the full budget, an estimate above the
// budget is clamped to the full budget (and runs alone), and every positive
// estimate within the budget is charged as-is. It never rejects a job.
func uploadMemoryClassify(estimate, budget int64) (weight int64, unknown, clamped bool) {
	switch {
	case estimate <= 0:
		return budget, true, false
	case estimate > budget:
		return budget, false, true
	default:
		return estimate, false, false
	}
}

// uploadMemoryAdmission is the private weighted admission core. It is safe for
// concurrent use; the embedded semaphore serializes accounting and wakes
// waiters in arrival order.
type uploadMemoryAdmission struct {
	budget int64
	source uploadMemorySource
	sem    *semaphore.Weighted
	hooks  uploadMemoryHooks
}

// newUploadMemoryAdmission builds an admission controller for an already
// resolved budget. It starts no goroutines.
func newUploadMemoryAdmission(budget uploadMemoryBudget, hooks uploadMemoryHooks) (*uploadMemoryAdmission, error) {
	if budget.Bytes <= 0 {
		return nil, fmt.Errorf("ingest: upload memory: budget must be positive, got %d", budget.Bytes)
	}
	return &uploadMemoryAdmission{
		budget: budget.Bytes,
		source: budget.Source,
		sem:    semaphore.NewWeighted(budget.Bytes),
		hooks:  hooks,
	}, nil
}

// newUploadMemoryAdmissionForHost resolves explicit (0 means auto) against the
// real cgroup hierarchy and runtime GOMEMLIMIT and builds an admission
// controller. It never changes the runtime memory limit.
func newUploadMemoryAdmissionForHost(explicit int64, hooks uploadMemoryHooks) (*uploadMemoryAdmission, error) {
	budget, err := resolveUploadMemoryBudget(explicit, hostUploadMemoryLimits())
	if err != nil {
		return nil, err
	}
	return newUploadMemoryAdmission(budget, hooks)
}

// Budget returns the resolved budget and its source.
func (a *uploadMemoryAdmission) Budget() uploadMemoryBudget {
	if a == nil {
		return uploadMemoryBudget{}
	}
	return uploadMemoryBudget{Bytes: a.budget, Source: a.source}
}

// Acquire admits one job whose estimated merge footprint is estimate bytes and
// returns a release function that is safe to call repeatedly; only the first
// call runs the release hook. A non-positive estimate is charged
// the full budget. An estimate above the budget is clamped to the full budget
// and waits until every other admitted job released, so it runs alone; it is
// never rejected for its size. Warning and Oversized report an oversized job
// only after it was admitted, so a request canceled while waiting is not
// counted; one canceled after admission is still counted. Acquire returns the
// context error only when ctx ends while waiting, and a context that is
// already done never acquires even if the budget is free.
func (a *uploadMemoryAdmission) Acquire(ctx context.Context, estimate int64) (release func(), err error) {
	if a == nil || a.sem == nil {
		return nil, errors.New("ingest: upload memory: admission is not initialized")
	}
	if ctx == nil {
		return nil, errors.New("ingest: upload memory: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("ingest: upload memory: %w", err)
	}

	lease := uploadMemoryLease{Estimate: estimate, Budget: a.budget}
	lease.Weight, lease.Unknown, lease.Clamped = uploadMemoryClassify(estimate, a.budget)

	if err := a.sem.Acquire(ctx, lease.Weight); err != nil {
		return nil, fmt.Errorf("ingest: upload memory: acquire %d of %d bytes: %w", lease.Weight, a.budget, err)
	}

	// A plain sync.Once is used instead of sync.OnceFunc on purpose: OnceFunc
	// re-panics on every call after the wrapped function panicked, which would
	// make a repeated release with a panicking hook panic again. sync.Once
	// marks the release complete even when the hook panics, so later calls are
	// no-ops.
	var once sync.Once
	release = func() {
		once.Do(func() {
			// Run the release bookkeeping before exposing the weight: otherwise
			// the next admitted job's Acquire hook can raise an active-jobs
			// gauge above the budget before this job's Release hook decremented
			// it. The deferred release still returns the weight if the hook
			// panics.
			defer a.sem.Release(lease.Weight)
			if a.hooks.Release != nil {
				a.hooks.Release(lease)
			}
		})
	}
	// The callbacks run only once the job is admitted, so the oversized counter
	// counts admitted attempts that were not canceled while waiting (a job
	// canceled after admission is still counted), and the active-jobs gauge
	// excludes admission waits. If a callback panics, return the weight before
	// re-raising so a bad hook cannot strand the budget.
	defer func() {
		if p := recover(); p != nil {
			release()
			panic(p)
		}
	}()
	if lease.Clamped {
		a.oversized(uploadMemoryWarning{Estimate: estimate, Budget: a.budget, Weight: a.budget})
	}
	if a.hooks.Acquire != nil {
		a.hooks.Acquire(lease)
	}
	return release, nil
}

// oversized reports an admitted oversized job through the warning and counter
// hooks.
func (a *uploadMemoryAdmission) oversized(w uploadMemoryWarning) {
	if a.hooks.Warning != nil {
		a.hooks.Warning(w)
	}
	if a.hooks.Oversized != nil {
		a.hooks.Oversized(w)
	}
}

// uploadMemoryLimits are the host readings the auto budget uses. A nil field
// means that source is unknown; production passes hostUploadMemoryLimits, so
// tests can inject deterministic values without touching the host.
type uploadMemoryLimits struct {
	// Cgroup returns the smallest finite effective cgroup memory limit in
	// bytes. ok is false when no finite limit applies.
	Cgroup func() (limit int64, ok bool)
	// GoMemoryLimit returns the runtime GOMEMLIMIT in bytes; math.MaxInt64
	// means no limit.
	GoMemoryLimit func() int64
}

// hostUploadMemoryLimits reads the real cgroup hierarchy and runtime memory
// limit. debug.SetMemoryLimit(-1) is the documented read-only form: a negative
// input does not adjust the limit.
func hostUploadMemoryLimits() uploadMemoryLimits {
	return uploadMemoryLimits{
		Cgroup: func() (int64, bool) {
			return readCgroupMemoryLimit("/proc/self/cgroup", "/proc/self/mountinfo", os.ReadFile)
		},
		GoMemoryLimit: func() int64 { return debug.SetMemoryLimit(-1) },
	}
}

// resolveUploadMemoryBudget turns an explicit budget or the auto policy into a
// positive budget. A positive explicit value wins. Otherwise the budget is
// half the smallest finite cgroup hierarchy limit or runtime GOMEMLIMIT,
// preferring the cgroup limit on a tie; when neither is known it falls back to
// uploadMemoryFallbackBudget. A negative explicit value is rejected.
func resolveUploadMemoryBudget(explicit int64, limits uploadMemoryLimits) (uploadMemoryBudget, error) {
	switch {
	case explicit < 0:
		return uploadMemoryBudget{}, fmt.Errorf("ingest: upload memory: explicit budget must not be negative, got %d", explicit)
	case explicit > 0:
		return uploadMemoryBudget{Bytes: explicit, Source: uploadMemorySourceExplicit}, nil
	}

	var (
		limit  int64
		source uploadMemorySource
	)
	consider := func(bytes int64, from uploadMemorySource) {
		// Non-positive readings and the canonical "unlimited" value do not
		// constrain the budget.
		if bytes <= 0 || bytes == math.MaxInt64 {
			return
		}
		if limit == 0 || bytes < limit {
			limit, source = bytes, from
		}
	}
	if limits.Cgroup != nil {
		if bytes, ok := limits.Cgroup(); ok {
			consider(bytes, uploadMemorySourceCgroup)
		}
	}
	if limits.GoMemoryLimit != nil {
		consider(limits.GoMemoryLimit(), uploadMemorySourceGoMemoryLimit)
	}
	if limit == 0 {
		return uploadMemoryBudget{Bytes: uploadMemoryFallbackBudget, Source: uploadMemorySourceFallback}, nil
	}
	// Keep at least one byte: a sub-two-byte limit must still admit jobs one at
	// a time rather than turn the semaphore into an unlimited one.
	return uploadMemoryBudget{Bytes: max(limit/2, 1), Source: source}, nil
}

// readCgroupMemoryLimit returns the smallest finite memory limit across the
// cgroup v2 and v1 hierarchies that applies to the process whose cgroup paths
// are listed in procCgroup. procMountinfo supplies the cgroup mounts, so a
// bind-mounted or delegated subtree maps its membership path onto the visible
// mountpoint instead of assuming the process lives at a hardcoded mount root.
// readFile is the filesystem seam. Missing, malformed or unlimited levels do
// not constrain.
func readCgroupMemoryLimit(procCgroup, procMountinfo string, readFile func(string) ([]byte, error)) (int64, bool) {
	if readFile == nil {
		return 0, false
	}
	procData, err := readFile(procCgroup)
	if err != nil {
		return 0, false
	}
	v2Path, hasV2, v1Path, hasV1 := parseSelfCgroup(procData)
	mountData, err := readFile(procMountinfo)
	if err != nil {
		return 0, false
	}

	var (
		limit int64
		found bool
	)
	consider := func(bytes int64, ok bool) {
		if ok && (!found || bytes < limit) {
			limit, found = bytes, true
		}
	}
	for _, mount := range parseCgroupMounts(mountData) {
		switch {
		case mount.fsType == "cgroup2" && hasV2:
			visible, ok := cgroupVisiblePath(v2Path, mount.root)
			if !ok {
				continue
			}
			consider(cgroupHierarchyMemoryLimit(mount.mountpoint, visible, "memory.max", readFile))
		case mount.fsType == "cgroup" && hasV1 && mount.memoryController():
			visible, ok := cgroupVisiblePath(v1Path, mount.root)
			if !ok {
				continue
			}
			consider(cgroupHierarchyMemoryLimit(mount.mountpoint, visible, "memory.limit_in_bytes", readFile))
		}
	}
	return limit, found
}

// cgroupMount is one cgroup mount from /proc/self/mountinfo: the filesystem
// type, the subtree root the mount exposes and where it is mounted.
type cgroupMount struct {
	fsType       string
	root         string
	mountpoint   string
	superOptions string
}

// memoryController reports whether a cgroup v1 mount carries the memory
// controller in its super options.
func (m cgroupMount) memoryController() bool {
	for option := range strings.SplitSeq(m.superOptions, ",") {
		if option == "memory" {
			return true
		}
	}
	return false
}

// parseCgroupMounts extracts the cgroup and cgroup2 mounts from mountinfo.
// Each line is
//
//	id parent major:minor root mountpoint options [optional...] - fstype source super-options
//
// so the separator field splits the mount attributes from the filesystem
// attributes. Escaped root and mountpoint fields are unescaped.
func parseCgroupMounts(data []byte) []cgroupMount {
	var mounts []cgroupMount
	for line := range strings.SplitSeq(strings.TrimSpace(string(data)), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 7 {
			continue
		}
		sep := slices.Index(fields, "-")
		if sep < 6 || sep+3 >= len(fields) {
			continue
		}
		mounts = append(mounts, cgroupMount{
			fsType:       fields[sep+1],
			root:         unescapeMountInfo(fields[3]),
			mountpoint:   unescapeMountInfo(fields[4]),
			superOptions: fields[sep+3],
		})
	}
	return mounts
}

// cgroupVisiblePath maps a cgroup membership path onto the visible subtree of a
// mount. It strips the mount root and reports ok=false when the membership path
// lies outside that root, in which case the mount does not show the process's
// cgroup.
func cgroupVisiblePath(membership, mountRoot string) (string, bool) {
	membership = cgroupCleanPath(membership)
	mountRoot = cgroupCleanPath(mountRoot)
	switch {
	case mountRoot == "/":
		return membership, true
	case membership == mountRoot:
		return "/", true
	case strings.HasPrefix(membership, mountRoot+"/"):
		return strings.TrimPrefix(membership, mountRoot), true
	default:
		return "", false
	}
}

// unescapeMountInfo decodes the octal escapes mountinfo uses for space, tab,
// newline and backslash in its path fields.
func unescapeMountInfo(field string) string {
	if !strings.ContainsRune(field, '\\') {
		return field
	}
	return strings.NewReplacer(
		`\040`, " ",
		`\011`, "\t",
		`\012`, "\n",
		`\134`, `\`,
	).Replace(field)
}

// parseSelfCgroup extracts the cgroup v2 and v1 memory paths from the contents
// of /proc/self/cgroup. The format is hierarchy-id:controller-list:cgroup-path;
// v2 is the "0::" line and v1 memory is the line whose controller list holds
// "memory".
func parseSelfCgroup(data []byte) (v2Path string, hasV2 bool, v1Path string, hasV1 bool) {
	for line := range strings.SplitSeq(strings.TrimSpace(string(data)), "\n") {
		id, rest, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		controllers, cgPath, ok := strings.Cut(rest, ":")
		if !ok {
			continue
		}
		switch {
		case id == "0" && controllers == "":
			if !hasV2 {
				v2Path, hasV2 = cgPath, true
			}
		case hasMemoryController(controllers):
			if !hasV1 {
				v1Path, hasV1 = cgPath, true
			}
		}
	}
	return v2Path, hasV2, v1Path, hasV1
}

// hasMemoryController reports whether a v1 controller list names the memory
// controller.
func hasMemoryController(controllers string) bool {
	for c := range strings.SplitSeq(controllers, ",") {
		if c == "memory" {
			return true
		}
	}
	return false
}

// cgroupHierarchyMemoryLimit walks rel and every ancestor up to the mount root
// under root and returns the smallest finite value of file. Walking the
// hierarchy matters because a process's own cgroup may be unlimited while an
// ancestor container cgroup is not.
func cgroupHierarchyMemoryLimit(root, rel, file string, readFile func(string) ([]byte, error)) (int64, bool) {
	if root == "" || readFile == nil {
		return 0, false
	}
	rel = cgroupCleanPath(rel)
	var (
		limit int64
		found bool
	)
	for {
		name := filepath.Join(root, filepath.FromSlash(rel), file)
		// A missing or unreadable level cannot be assumed to constrain, so it
		// is skipped and the walk continues to a readable ancestor. If nothing
		// is readable the caller keeps the conservative fallback budget.
		if data, err := readFile(name); err == nil {
			if bytes, ok := parseCgroupLimit(file, data); ok && (!found || bytes < limit) {
				limit, found = bytes, true
			}
		}
		if rel == "/" {
			break
		}
		rel = path.Dir(rel)
	}
	return limit, found
}

// cgroupCleanPath normalizes a /proc/self/cgroup path to an absolute slash
// path rooted at the mount.
func cgroupCleanPath(p string) string {
	if p == "" {
		return "/"
	}
	return path.Clean("/" + p)
}

// parseCgroupLimit parses one limit file. "max" and the v1 unlimited sentinel
// mean unlimited; malformed and non-positive values are ignored.
func parseCgroupLimit(file string, data []byte) (int64, bool) {
	text := strings.TrimSpace(string(data))
	if text == "" || text == "max" {
		return 0, false
	}
	bytes, err := strconv.ParseInt(text, 10, 64)
	if err != nil || bytes <= 0 {
		return 0, false
	}
	if file == "memory.limit_in_bytes" && bytes >= uploadMemoryCgroupV1UnlimitedFloor {
		return 0, false
	}
	return bytes, true
}

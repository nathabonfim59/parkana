package main

import (
	"io"
	"log/slog"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"go.yaml.in/yaml/v3"

	"github.com/nathabonfim59/parkana/internal/app"
	"github.com/nathabonfim59/parkana/internal/config"
)

// specPath is the OpenAPI document of every route the process serves,
// relative to this package directory.
const specPath = "../../api/parkana.yaml"

// assembleForTest assembles a process from args, without serving, on a
// temporary data dir with an fs object store and a memory catalog. Roles
// register their collectors on metrics; the server registers on and gathers
// from gather. Cleanup releases the role services and the assembly.
func assembleForTest(t *testing.T, metrics prometheus.Registerer, gather *prometheus.Registry, args ...string) *assembly {
	t.Helper()
	base := []string{"-data.dir=" + t.TempDir(), "-http.addr=127.0.0.1:0", "-catalog.driver=memory", "-storage.backend=fs"}
	cfg, err := config.Load(append(base, args...), func(string) string { return "" }, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	a, err := assemble(t.Context(), cfg, slog.New(slog.DiscardHandler), metrics, gather)
	if err != nil {
		t.Fatalf("assemble %q: %v", args, err)
	}
	t.Cleanup(func() {
		// The services never ran: release them the way a failed startup does.
		// The HTTP server holds nothing until it serves, so it is left out.
		var roles []app.Service
		for name, svc := range a.services {
			if name != "http" {
				roles = append(roles, svc)
			}
		}
		if err := releaseServices(roles); err != nil {
			t.Errorf("release services: %v", err)
		}
		a.close()
	})
	return a
}

// specOperations returns every operation of the spec as "METHOD path". The
// only normalization is the method: OpenAPI writes it as a lowercase key
// under the path, a ServeMux pattern as an uppercase prefix. Paths are
// compared verbatim, because both use the same {name} wildcard syntax.
func specOperations(t *testing.T) []string {
	t.Helper()
	raw, err := os.ReadFile(specPath)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Paths map[string]map[string]yaml.Node `yaml:"paths"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("%s: %v", specPath, err)
	}
	methods := []string{"get", "put", "post", "delete", "options", "head", "patch", "trace"}
	var ops []string
	for path, item := range doc.Paths {
		for key := range item {
			if slices.Contains(methods, key) {
				ops = append(ops, strings.ToUpper(key)+" "+path)
			}
		}
	}
	slices.Sort(ops)
	return ops
}

// TestRoutesMatchSpec fails when a route the all-in-one process registers is
// missing from api/parkana.yaml, or the spec documents a method and path no
// route serves. The process is assembled with every role that has a factory,
// so the pipeline API, the SQL API and live tail (with its loopback ingester)
// are all registered, and every spec operation must be reachable.
func TestRoutesMatchSpec(t *testing.T) {
	reg := prometheus.NewRegistry()
	a := assembleForTest(t, reg, reg, "-roles=all")
	patterns := a.srv.Patterns()
	for _, p := range patterns {
		if method, _, ok := strings.Cut(p, " "); !ok || method != strings.ToUpper(method) {
			t.Errorf("pattern %q has no method; the spec needs one operation per method", p)
		}
	}
	spec := specOperations(t)
	served := setOf(patterns)
	documented := setOf(spec)
	for _, p := range patterns {
		if !documented[p] {
			t.Errorf("route %q is served but missing from %s", p, specPath)
		}
	}
	for _, op := range spec {
		if !served[op] {
			t.Errorf("%s documents %q, which the all-in-one process does not serve", specPath, op)
		}
	}
	t.Logf("%d routes served, %d operations documented", len(patterns), len(spec))
}

// TestRoleRoutesInSpec checks every single-role process: each route it
// serves must be documented.
func TestRoleRoutesInSpec(t *testing.T) {
	documented := setOf(specOperations(t))
	for _, role := range []string{"ingester", "querier", "compactor"} {
		t.Run(role, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			a := assembleForTest(t, reg, reg, "-roles="+role)
			for _, p := range a.srv.Patterns() {
				if !documented[p] {
					t.Errorf("-roles=%s serves %q, which is missing from %s", role, p, specPath)
				}
			}
		})
	}
}

func setOf(items []string) map[string]bool {
	set := make(map[string]bool, len(items))
	for _, s := range items {
		set[s] = true
	}
	return set
}

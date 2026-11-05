package logs

import (
	"testing"
)

func TestNewSortsAndDedups(t *testing.T) {
	ls := New(Label{"b", "1"}, Label{"a", "x"}, Label{"b", "2"})
	if got, want := ls.String(), `{a="x", b="2"}`; got != want {
		t.Fatalf("String() = %s, want %s", got, want)
	}
	if err := ls.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestFromMapHashStable(t *testing.T) {
	a := FromMap(map[string]string{"job": "api", "env": "prod"})
	b := New(Label{"env", "prod"}, Label{"job", "api"})
	if a.Hash() != b.Hash() {
		t.Fatal("equal label sets hash differently")
	}
	c := FromMap(map[string]string{"job": "apienv", "": "prod"})
	if a.Hash() == c.Hash() {
		t.Fatal("hash ignores name/value boundaries")
	}
}

func TestGetSplit(t *testing.T) {
	ls := FromMap(map[string]string{"job": "api", "level": "info", "pod": "p1"})
	if v, ok := ls.Get("level"); !ok || v != "info" {
		t.Fatalf("Get(level) = %q, %v", v, ok)
	}
	if _, ok := ls.Get("nope"); ok {
		t.Fatal("Get(nope) found")
	}
	kept, rest := ls.Split([]string{"level", "pod"})
	if kept.String() != `{level="info", pod="p1"}` || rest.String() != `{job="api"}` {
		t.Fatalf("Split = %s / %s", kept, rest)
	}
}

func TestValidate(t *testing.T) {
	for _, name := range []string{"", "1a", "a-b", "a.b"} {
		if err := (Labels{{name, "v"}}).Validate(); err == nil {
			t.Errorf("Validate accepted name %q", name)
		}
	}
	if err := (Labels{{"b", ""}, {"a", ""}}).Validate(); err == nil {
		t.Error("Validate accepted unsorted labels")
	}
}

func TestStreamName(t *testing.T) {
	keys := []string{"service_name", "job"}
	tests := []struct {
		ls   Labels
		want string
	}{
		{FromMap(map[string]string{"service_name": "Checkout API", "job": "x"}), "checkout_api"},
		{FromMap(map[string]string{"job": "ns/app"}), "ns_app"},
		{FromMap(map[string]string{"service_name": "", "job": "j"}), "j"},
		{FromMap(map[string]string{"other": "x"}), DefaultStream},
	}
	for _, tt := range tests {
		if got := tt.ls.StreamName(keys); got != tt.want {
			t.Errorf("StreamName(%s) = %q, want %q", tt.ls, got, tt.want)
		}
	}
}

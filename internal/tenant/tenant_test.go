package tenant

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestResolve(t *testing.T) {
	tests := []struct {
		header      string
		multiTenant bool
		want        ID
		wantErr     error
	}{
		{header: "", want: Default},
		{header: "default", want: Default},
		{header: "acme", wantErr: ErrNotPermitted},
		{header: "acme", multiTenant: true, want: "acme"},
		{header: "a/b", multiTenant: true, wantErr: ErrInvalid},
		{header: "..", multiTenant: true, wantErr: ErrInvalid},
		{header: strings.Repeat("x", 65), multiTenant: true, wantErr: ErrInvalid},
	}
	for _, tt := range tests {
		got, err := Resolve(tt.header, tt.multiTenant)
		if !errors.Is(err, tt.wantErr) {
			t.Errorf("Resolve(%q, %v) error = %v, want %v", tt.header, tt.multiTenant, err, tt.wantErr)
		}
		if got != tt.want {
			t.Errorf("Resolve(%q, %v) = %q, want %q", tt.header, tt.multiTenant, got, tt.want)
		}
	}
}

func TestMiddleware(t *testing.T) {
	var seen ID
	h := Middleware(false, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = From(r.Context())
	}))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusOK || seen != Default {
		t.Fatalf("no header: code %d tenant %q", rec.Code, seen)
	}

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(Header, "other")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("foreign tenant: code %d, want 401", rec.Code)
	}
}

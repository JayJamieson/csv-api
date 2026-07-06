package db

import "testing"

func TestResolveTransform(t *testing.T) {
	cases := []struct {
		format  string
		wantErr bool
	}{
		{"", false}, // omitted format: must default to objects, not panic
		{"objects", false},
		{"array", false},
		{"bogus", true},   // unrecognized format: must error, not yield a nil func
		{"Objects", true}, // case-sensitive: not a silent fallback
	}
	for _, c := range cases {
		fn, err := resolveTransform(c.format)
		if c.wantErr {
			if err == nil {
				t.Errorf("format %q: expected an error, got none", c.format)
			}
			if fn != nil {
				t.Errorf("format %q: expected nil func alongside the error", c.format)
			}
			continue
		}
		if err != nil {
			t.Errorf("format %q: unexpected error: %v", c.format, err)
		}
		if fn == nil {
			t.Fatalf("format %q: got nil transform func with no error (this is the exact bug: "+
				"a nil func here panics when called)", c.format)
		}
	}
}

func TestResolveTransformDefaultMatchesObjects(t *testing.T) {
	// The API spec documents format's default as "objects"; confirm the
	// empty-format case actually produces the same function as an explicit
	// "objects", not just "some non-nil function".
	defaultFn, err := resolveTransform("")
	if err != nil {
		t.Fatal(err)
	}
	cols := []string{"a"}
	vals := []any{"x"}
	got := defaultFn(cols, vals)
	want := transformObject(cols, vals)
	gm, gok := got.(map[string]any)
	wm, wok := want.(map[string]any)
	if !gok || !wok || gm["a"] != wm["a"] {
		t.Errorf("default format: got %#v, want objects shape %#v", got, want)
	}
}

package logs

import (
	"testing"
)

func TestParseLabelsValid(t *testing.T) {
	cases := []struct {
		in   string
		want string // String() form of the expected set
	}{
		{`{}`, `{}`},
		{`{   }`, `{}`},
		{`{a="b"}`, `{a="b"}`},
		{`{ a = "b" }`, `{a="b"}`},
		{`{b="1", a="2"}`, `{a="2", b="1"}`},
		{"{\n\ta\t=\t\"b\" ,\r\n c = \"d\" }", `{a="b", c="d"}`},
		{`{a=""}`, `{a=""}`},
		{`{_x9="v"}`, `{_x9="v"}`},
		{`{a="b\"c"}`, `{a="b\"c"}`},
		{`{a="b\nc"}`, `{a="b\nc"}`},
		{`{a="\\"}`, `{a="\\"}`},
		{`{a="é"}`, `{a="é"}`},
		{`{a="café"}`, `{a="café"}`},
		{"{a=`raw`}", `{a="raw"}`},
		{"{a=`} inside`}", `{a="} inside"}`},
		{`{a="} ,"}  `, `{a="} ,"}`},
	}
	for _, c := range cases {
		got, err := ParseLabels(c.in)
		if err != nil {
			t.Errorf("ParseLabels(%q) error: %v", c.in, err)
			continue
		}
		if s := got.String(); s != c.want {
			t.Errorf("ParseLabels(%q) = %s, want %s", c.in, s, c.want)
		}
	}
}

func TestParseLabelsInvalid(t *testing.T) {
	cases := []string{
		``,
		`a="b"`,
		`{a="b"`,
		`{a="b"} x`,
		`{a="b"} {c="d"}`,
		`{a}`,      // bare name: a matcher without a value
		`{a!="b"}`, // match operator
		`{a=~"b"}`, // match operator
		`{a!~"b"}`, // match operator
		`{a="b",}`, // trailing comma
		`{,a="b"}`,
		`{a="b" "c"}`,
		`{1a="b"}`,       // invalid name
		`{a-b="c"}`,      // invalid name
		`{"a"="b"}`,      // quoted name
		`{a=b}`,          // unquoted value
		`{a='b'}`,        // single-quoted value
		`{a="b}`,         // unterminated value
		"{a=`b}",         // unterminated raw value
		`{a="b\"}`,       // unterminated escape
		`{a="1", a="2"}`, // duplicate name
		`{a="1", a="1"}`, // duplicate name, same value
		`{a="1", b="2", a="3"}`,
	}
	for _, in := range cases {
		if got, err := ParseLabels(in); err == nil {
			t.Errorf("ParseLabels(%q) = %v, want error", in, got)
		}
	}
}

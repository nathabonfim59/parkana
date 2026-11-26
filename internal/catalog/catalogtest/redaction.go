package catalogtest

import "strings"

// SecretMarker is part of every secret in RedactionCases, in every
// whitespace- or punctuation-separated piece of it, so a leak of any piece,
// raw or decoded, shows up as the marker in the redacted text.
const SecretMarker = "S3cr3t"

// RedactionCase is a Postgres DSN that holds secrets (password,
// sslpassword), for tests that check a redactor hides them. The DSNs point at
// 127.0.0.1:1, where nothing listens, or fail to parse, so opening one always
// fails.
type RedactionCase struct {
	// Name names the case in subtests.
	Name string
	// DSN is the connection string, URL or keyword/value form.
	DSN string
	// Secrets lists every secret in DSN as written and, where it differs,
	// as decoded (percent escapes, + in a query, backslash escapes in a
	// quoted keyword value). Each contains SecretMarker.
	Secrets []string
}

// Leak returns a secret of c found in s, or SecretMarker if s holds only a
// piece of one, and whether s leaks anything.
func (c RedactionCase) Leak(s string) (string, bool) {
	for _, secret := range c.Secrets {
		if strings.Contains(s, secret) {
			return secret, true
		}
	}
	if strings.Contains(s, SecretMarker) {
		return SecretMarker, true
	}
	return "", false
}

// RedactionCases are the DSNs every Postgres DSN redactor must handle: the
// catalog's error wrapping (catalog.NewPostgres) and the config package's
// redaction of flag errors and logs. Keeping one table keeps them from
// drifting apart.
var RedactionCases = []RedactionCase{
	// URL form, unreachable host.
	{"url encoded @ : space", "postgres://u:S3cr3t%40S3cr3t%3AS3cr3t%20S3cr3t@127.0.0.1:1/db?sslmode=disable&connect_timeout=2",
		[]string{"S3cr3t%40S3cr3t%3AS3cr3t%20S3cr3t", "S3cr3t@S3cr3t:S3cr3t S3cr3t"}},
	{"url raw @ in password", "postgres://u:S3cr3t@S3cr3t@127.0.0.1:1/db?sslmode=disable&connect_timeout=2",
		[]string{"S3cr3t@S3cr3t"}},
	{"url postgresql scheme", "postgresql://u:S3cr3tpw@127.0.0.1:1/db?sslmode=disable&connect_timeout=2",
		[]string{"S3cr3tpw"}},
	{"url password query param", "postgres://u@127.0.0.1:1/db?password=S3cr3t%26S3cr3t%20S3cr3t%2BS3cr3t&sslmode=disable&connect_timeout=2",
		[]string{"S3cr3t%26S3cr3t%20S3cr3t%2BS3cr3t", "S3cr3t&S3cr3t S3cr3t+S3cr3t"}},
	{"url password query param plus", "postgres://u@127.0.0.1:1/db?password=S3cr3t+S3cr3t&sslmode=disable&connect_timeout=2",
		[]string{"S3cr3t+S3cr3t", "S3cr3t S3cr3t"}},
	{"url sslpassword query param", "postgres://u:S3cr3tpw@127.0.0.1:1/db?sslpassword=S3cr3tssl&sslmode=disable&connect_timeout=2",
		[]string{"S3cr3tpw", "S3cr3tssl"}},
	// Keyword form, unreachable host.
	{"keyword bare", "host=127.0.0.1 port=1 user=u password=S3cr3tbare dbname=db sslmode=disable connect_timeout=2",
		[]string{"S3cr3tbare"}},
	{"keyword quoted with space and escapes", `host=127.0.0.1 port=1 user=u password='S3cr3tA S3cr3tB\'S3cr3tC\\S3cr3tD' dbname=db sslmode=disable connect_timeout=2`,
		[]string{`S3cr3tA S3cr3tB\'S3cr3tC\\S3cr3tD`, `S3cr3tA S3cr3tB'S3cr3tC\S3cr3tD`}},
	{"keyword spaces around =", "host=127.0.0.1 port=1 user=u password = S3cr3tspaced dbname=db sslmode=disable connect_timeout=2",
		[]string{"S3cr3tspaced"}},
	{"keyword quoted spaces around =", "host=127.0.0.1 port=1 user=u password = 'S3cr3tA S3cr3tB' dbname=db sslmode=disable connect_timeout=2",
		[]string{"S3cr3tA S3cr3tB"}},
	{"keyword sslpassword bare", "host=127.0.0.1 port=1 user=u password=S3cr3tpw sslpassword=S3cr3tssl dbname=db sslmode=disable connect_timeout=2",
		[]string{"S3cr3tpw", "S3cr3tssl"}},
	{"keyword sslpassword quoted", "host=127.0.0.1 port=1 user=u sslpassword='S3cr3tA S3cr3tB' dbname=db sslmode=disable connect_timeout=2",
		[]string{"S3cr3tA S3cr3tB"}},
	{"keyword password first", "password=S3cr3tfirst host=127.0.0.1 port=1 user=u dbname=db sslmode=disable connect_timeout=2",
		[]string{"S3cr3tfirst"}},
	// Bad syntax: the parser rejects the DSN and may echo it.
	{"url bad percent escape", "postgres://u:S3cr3t%zzS3cr3t@127.0.0.1:1/db",
		[]string{"S3cr3t%zzS3cr3t"}},
	{"url bad port", "postgres://u:S3cr3tpw@127.0.0.1:notaport/db",
		[]string{"S3cr3tpw"}},
	{"url bad query escape", "postgres://u@127.0.0.1:1/db?password=S3cr3t%zzS3cr3t",
		[]string{"S3cr3t%zzS3cr3t"}},
	{"url bad sslmode", "postgres://u:S3cr3tpw@127.0.0.1:1/db?sslmode=bogus",
		[]string{"S3cr3tpw"}},
	{"keyword bad sslmode", "host=127.0.0.1 port=1 password=S3cr3tpw sslmode=bogus",
		[]string{"S3cr3tpw"}},
	{"keyword quoted bad sslmode", "host=127.0.0.1 port=1 password='S3cr3tA S3cr3tB' sslmode=bogus",
		[]string{"S3cr3tA S3cr3tB"}},
	// pgx's own redaction stops at the first space of an unterminated
	// quoted value.
	{"keyword unterminated quote", "host=127.0.0.1 port=1 password='S3cr3tA S3cr3tB",
		[]string{"S3cr3tA S3cr3tB"}},
	{"keyword unterminated quote three parts", "host=127.0.0.1 port=1 password='S3cr3tA S3cr3tB S3cr3tC",
		[]string{"S3cr3tA S3cr3tB S3cr3tC"}},
	{"keyword unterminated quote escaped quote", `host=127.0.0.1 port=1 password='S3cr3tA\' S3cr3tB S3cr3tC`,
		[]string{`S3cr3tA\' S3cr3tB S3cr3tC`, `S3cr3tA' S3cr3tB S3cr3tC`}},
	{"keyword unterminated sslpassword", "host=127.0.0.1 port=1 sslpassword='S3cr3tA S3cr3tB",
		[]string{"S3cr3tA S3cr3tB"}},
	{"keyword unterminated quote after password", "host=127.0.0.1 port=1 password=S3cr3tpw sslpassword='S3cr3tA S3cr3tB",
		[]string{"S3cr3tpw", "S3cr3tA S3cr3tB"}},
	{"keyword bad port", "host=127.0.0.1 port=notaport password=S3cr3tpw",
		[]string{"S3cr3tpw"}},
}

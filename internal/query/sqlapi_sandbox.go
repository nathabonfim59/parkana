// The SQL API's request-owned DuckDB sandbox: one isolated in-memory database
// per request that materializes the tenant's in-range rows as a logs table and
// then locks itself down before any user SQL is prepared (PLAN.md decision 16).
//
// The design is enforcement by DuckDB, plus layered statement validation: a
// lexical text preflight, a keyword policy, a parse-only serialization check
// and an AST content walk (see validate).
//
//   - Each request gets its own in-memory database, so the database-global
//     memory_limit and threads settings (DuckDB custom_settings.cpp:1213-1231
//     in the pinned build) budget exactly one request and never touch the
//     shared LogQL handle.
//   - Rows are materialized into a plain table while file access is still
//     allowed, every object lease is released, and only then are external
//     access and the local filesystem disabled and the configuration locked.
//     No file path remains reachable from user SQL at all, so no query can
//     bypass the request's time range by reading a listed file directly —
//     DBConfig::CanAccessFile (config.cpp:1017-1055) allows exact allowed
//     paths, which is why materialization, not a lazy view over cached files,
//     is the only accepted design.
//   - All one-way settings are applied before the first user statement and
//     verified by tests: re-enabling external access throws
//     (custom_settings.cpp:669-671), a previously disabled filesystem cannot
//     be re-enabled (virtual_file_system.cpp:79-84), allowed paths and
//     directories cannot change once external access is off
//     (custom_settings.cpp:324-391), and lock_configuration rejects every
//     later setting change except schema/search_path (config.cpp:661-681).
package query

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/duckdb/duckdb-go/v2"
)

// sqlSandboxDSN opens the per-request in-memory database with the two
// startup-only extension settings pinned to their safe values:
// allow_community_extensions defaults to true in the pinned build and cannot
// be changed while the database is running (custom_settings.cpp:185), so it
// must be set at open time; allow_unsigned_extensions already defaults to
// false (custom_settings.cpp:290) and is pinned explicitly for depth.
const sqlSandboxDSN = ":memory:?allow_community_extensions=false&allow_unsigned_extensions=false"

// sqlLockdownStatements returns the one-way sandbox settings, in dependency
// order, applied after materialization is complete and every object lease
// has been released. A function returning a fresh literal keeps the package
// free of mutable state.
//
// autoinstall/autoload and persistent secrets (config.cpp:97-99,
// custom_settings.cpp:242-259) stop user SQL from fetching extensions or
// persisting secrets; disabled_filesystems removes LocalFileSystem itself —
// the layer that blocks read_parquet/read_csv/glob/replacement scans over the
// object cache's paths inside an otherwise valid SELECT. lock_configuration
// runs last, after every setting this sandbox owns has its final value; it
// rejects every later setting change except schema/search_path
// (config.cpp:661-681).
func sqlLockdownStatements() []string {
	return []string{
		"SET autoinstall_known_extensions=false",
		"SET autoload_known_extensions=false",
		"SET allow_persistent_secrets=false",
		"SET enable_external_access=false",
		"SET disabled_filesystems='LocalFileSystem'",
		"SET lock_configuration=true",
	}
}

// sqlSandbox is one request's isolated DuckDB database: a single connection
// over an in-memory database, from budgeting through lockdown to user SQL
// execution. It is never shared between requests.
type sqlSandbox struct {
	db   *sql.DB
	conn *sql.Conn
}

// openSQLSandbox opens the request's database and takes its one connection.
// The context covers the connect; every later step re-checks it.
func openSQLSandbox(ctx context.Context) (*sqlSandbox, error) {
	connector, err := duckdb.NewConnector(sqlSandboxDSN, nil)
	if err != nil {
		return nil, fmt.Errorf("query: sql: open sandbox: %w", err)
	}
	db := sql.OpenDB(connector)
	conn, err := db.Conn(ctx)
	if err != nil {
		err = fmt.Errorf("query: sql: sandbox connect: %w", err)
		return nil, errors.Join(err, db.Close())
	}
	return &sqlSandbox{db: db, conn: conn}, nil
}

// close releases the connection and the database behind it. It is idempotent
// enough for deferred use: closing an already closed *sql.DB or *sql.Conn
// returns an error that the caller joins but nothing panics.
func (s *sqlSandbox) close() error {
	return errors.Join(s.conn.Close(), s.db.Close())
}

// execSetting runs one trusted settings statement, naming it in the error.
func (s *sqlSandbox) execSetting(ctx context.Context, stmt string) error {
	if _, err := s.conn.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("query: sql: sandbox %s: %w", strings.TrimPrefix(stmt, "SET "), sqlInterrupted(ctx, err))
	}
	return nil
}

// sqlInterrupted joins err with the context's own cancellation error when ctx
// has ended. DuckDB reports a cancellation as its own "INTERRUPT Error:
// Interrupted!" that does not wrap context.Canceled or context.DeadlineExceeded,
// so without this a timed-out request would be classified as a SQL failure
// instead of a deadline.
func sqlInterrupted(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return errors.Join(err, ctxErr)
	}
	return err
}

// applyLimits sets the request budget before anything is loaded: UTC session
// time (the AGENTS.md timestamps contract), the database-global memory and
// thread budgets, and an empty temporary directory. The spill directory must
// be cleared before the first byte is materialized — an in-memory database
// otherwise spills to ".tmp" in the working directory under load
// (config.cpp:651-655), writing tenant data to disk and defeating the memory
// budget — and before enable_external_access is turned off, which permits the
// current temporary directory (custom_settings.cpp:684-687) and freezes it:
// a later temp_directory change throws a PermissionException
// (custom_settings.cpp:1654-1659). With the directory empty, buffer-manager
// overflow becomes an "Out of Memory Error" instead of a spill, which the
// executor maps to the typed sql.memory-limit error. memory_limit is spelled
// in plain bytes; every value comes from operator configuration, never from
// user input. The budget bounds DuckDB's buffer-manager accounting, not the
// process's OS RSS: transient overhang (allocator slack, result encoding in
// the HTTP layer) is possible and is a documented limit, not a hard process
// cap.
func (s *sqlSandbox) applyLimits(ctx context.Context, memBytes int64, threads int) error {
	for _, stmt := range []string{
		"SET TimeZone='UTC'",
		fmt.Sprintf("SET memory_limit='%dB'", memBytes),
		fmt.Sprintf("SET threads=%d", threads),
		"SET temp_directory=''",
		// Secondary depth guard behind the lexical preflight: the parser
		// itself refuses expression nesting over 1000.
		"SET max_expression_depth=1000",
	} {
		if err := s.execSetting(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

// materialize builds the logs relation: every row of the tenant's listed
// files whose _ts falls in [from, through), stored as a plain table named
// logs. Bound paths and timestamps make the statement parameterized end to
// end; union_by_name merges files whose promoted label columns differ (all
// promoted columns are VARCHAR, so the union never meets a type conflict).
// Rows are the stored rows exactly: no LogQL normalization, no deduplication
// — SQL sees stored data, retry duplicates included.
//
// With no files the table is created empty from the base schema, so user SQL
// has the same relation shape (read_parquet rejects an empty path list).
//
// A DuckDB out-of-memory failure here — the expected outcome when the range
// holds more than the budget can materialize, given the spill directory was
// never enabled — becomes errSQLMemory for the caller to report as a
// limit error naming sql.memory-limit.
func (s *sqlSandbox) materialize(ctx context.Context, paths []string, from, through time.Time) error {
	var stmt string
	var args []any
	if len(paths) == 0 {
		stmt = fmt.Sprintf("CREATE TABLE %s (%s TIMESTAMPTZ, %s VARCHAR, %s MAP(VARCHAR, VARCHAR), %s MAP(VARCHAR, VARCHAR))",
			quoteIdent("logs"), quoteIdent("_ts"), quoteIdent("line"), quoteIdent("labels"), quoteIdent("meta"))
	} else {
		stmt = fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM read_parquet([%s], union_by_name = true) WHERE %s >= ? AND %s < ?",
			quoteIdent("logs"), placeholders(len(paths)), quoteIdent("_ts"), quoteIdent("_ts"))
		args = make([]any, 0, len(paths)+2)
		for _, p := range paths {
			args = append(args, p)
		}
		// Stored _ts values are whole microseconds; bounds round up to the µs
		// grid so nanosecond boundaries never truncate onto the wrong side
		// (same rule as the LogQL source, sql.go groupSubquery).
		args = append(args,
			time.UnixMicro(ceilMicro(from)).UTC(),
			time.UnixMicro(ceilMicro(through)).UTC())
	}
	if _, err := s.conn.ExecContext(ctx, stmt, args...); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return fmt.Errorf("query: sql: materialize %d files: %w", len(paths), errors.Join(err, ctxErr))
		}
		if isOutOfMemory(err) {
			return fmt.Errorf("%w %w", errSQLMemory, err)
		}
		return fmt.Errorf("query: sql: materialize %d files: %w", len(paths), err)
	}
	return nil
}

// lockdown applies the one-way sandbox settings, in the order documented on
// sqlLockdownStatements, after materialization is complete and every object
// lease has been released.
func (s *sqlSandbox) lockdown(ctx context.Context) error {
	for _, stmt := range sqlLockdownStatements() {
		if err := s.execSetting(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

// validate proves the user's query is one side-effect-free SELECT over the
// sandbox's own data before the executor runs it, failing closed on
// independent layers:
//
//   - The text passes the lexical preflight (byte cap, nesting depth and the
//     explicit WITH RECURSIVE rejection) before DuckDB sees it at all.
//
//   - The statement must lead with the SELECT or WITH keyword. DuckDB reports
//     PRAGMA statements as statement type SELECT in the pinned build
//     (probe-verified), so a keyword-level policy — not a type check — is
//     what rejects them cheaply. Statements that are valid SQL but lead with
//     anything else (TABLE, VALUES, a parenthesized query) are rejected too:
//     conservative, and documented.
//
//   - The query is then parsed — never executed — by DuckDB's own
//     json_serialize_sql, with the text passed as a bound parameter through
//     QueryContext so the parse itself runs under the driver's cancellation
//     interruption. The pinned implementation
//     (extension/json/json_functions/json_serialize_sql.cpp:95-115) calls
//     Parser::ParseQuery and serializes; any statement whose type is not
//     SELECT throws inside the function ("Only SELECT statements can be
//     serialized to json!", line 101-103) — this is what rejects PRAGMA
//     rewrites, SET, CREATE, ATTACH, COPY and WITH-prefixed writes — and a
//     parse failure is reported in the returned JSON instead of thrown. The
//     executor accepts only {"error": false, "statements": [exactly one]}.
//
//   - The serialized statement is walked (sqlValidateAST) for the content
//     policy: only the logs relation and the query's own CTEs may appear as
//     named relations, and no settings/introspection/indirection or
//     file-reading function may appear anywhere in the tree — through
//     quoting, casing, CTEs, subqueries, lateral joins or windows alike.
//
// These layers together also settle the PrepareContext hazard: user SQL is
// never handed to Prepare/PrepareContext for validation, so nothing but the
// final single-statement QueryContext — whose earlier-statements loop is a
// no-op for one statement — can ever run it. Interrupt coverage is complete
// for the validation parse (json_serialize_sql runs as a query, and a
// deadline aborts it through the driver's interruption) and for statement
// execution. The residual is the re-parse inside the execution QueryContext:
// Connection::Prepare parsing is not actively interrupted, so its cost is
// bounded ahead of it — by the query text cap and lexical depth preflight
// and DuckDB's max_expression_depth=1000 — rather than by the deadline, and
// the same text was already parsed under interruptible coverage during
// validation. Bound parameters ($1, ?) are rejected at execution by
// database/sql's argument-count check with no arguments to bind.
func (s *sqlSandbox) validate(ctx context.Context, query string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("query: sql: validate: %w", err)
	}
	if err := sqlPreflight(query); err != nil {
		return err
	}
	if kw := sqlLeadKeyword(query); kw != "select" && kw != "with" {
		return fmt.Errorf("%w: statement must start with SELECT or WITH, leads with %q", errSQLStatement, kw)
	}
	var out any
	if err := s.conn.QueryRowContext(ctx, "SELECT json_serialize_sql(?)", query).Scan(&out); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			// The parse was interrupted by the deadline: the context error is
			// the classification callers need, not a client SQL error.
			return fmt.Errorf("query: sql: validate: %w", errors.Join(err, ctxErr))
		}
		if isOutOfMemory(err) {
			return fmt.Errorf("%w %w", errSQLMemory, err)
		}
		// The validator failed on the user's own text.
		return fmt.Errorf("%w: validate parse: %w", errSQLRejected, err)
	}
	obj, ok := out.(map[string]any)
	if !ok {
		return fmt.Errorf("query: sql: validate parse: unexpected result type %T", out)
	}
	if failed, _ := obj["error"].(bool); failed {
		kind, _ := obj["error_type"].(string)
		msg, _ := obj["error_message"].(string)
		return fmt.Errorf("%w: %s: %s", errSQLStatement, kind, msg)
	}
	stmts, _ := obj["statements"].([]any)
	if len(stmts) != 1 {
		return fmt.Errorf("%w: %d statements, want exactly one", errSQLStatement, len(stmts))
	}
	stmt, ok := stmts[0].(map[string]any)
	if !ok {
		return fmt.Errorf("query: sql: validate parse: unexpected statement shape %T", stmts[0])
	}
	return sqlValidateAST(ctx, stmt)
}

// query executes the already-validated user SELECT and returns its rows.
// QueryContext parses, plans and runs the single statement inside the
// driver's cancellation-interruption scope (duckdb-go statement.go:697-710
// and connection.go:81-109); for one statement the multi-statement
// execute-earlier loop in between is a no-op, so nothing but the validated
// SELECT ever runs.
func (s *sqlSandbox) query(ctx context.Context, query string) (*sql.Rows, error) {
	rows, err := s.conn.QueryContext(ctx, query)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, fmt.Errorf("query: sql: execute: %w", errors.Join(err, ctxErr))
		}
		if isOutOfMemory(err) {
			return nil, fmt.Errorf("%w %w", errSQLMemory, err)
		}
		return nil, fmt.Errorf("%w: %w", errSQLRejected, err)
	}
	return rows, nil
}

// sqlLeadKeyword returns the first keyword of query through the shared
// scanner: whitespace, strings and (nested) comments skipped, the following
// identifier lowered. Mis-lexing an ambiguous form can only surface a later
// word, so the caller fails closed; comments cannot hide a real lead because
// a statement's kind is decided by its first keyword. A punctuation lead
// (e.g. "(SELECT ...)") returns "".
func sqlLeadKeyword(query string) string {
	s := sqlScanner{q: query}
	word, _ := s.next()
	// A punctuation lead (including a bare dollar) or end of input yields no
	// word: fail closed.
	return word
}

// isOutOfMemory reports whether err is DuckDB's out-of-memory failure, read
// from the typed error the driver builds from DuckDB's message prefix
// (duckdb-go errors.go: *duckdb.Error with ErrorTypeOutOfMemory).
func isOutOfMemory(err error) bool {
	derr, ok := errors.AsType[*duckdb.Error](err)
	return ok && derr.Type == duckdb.ErrorTypeOutOfMemory
}

// errSQLMemory marks a materialization that exceeded the DuckDB memory
// budget. The HTTP layer maps it to 400 naming sql.memory-limit and
// suggesting a narrower time range.
var errSQLMemory = errors.New("query: sql: materialization exceeded the sql.memory-limit budget; use a narrower time range")

// errSQLText rejects query text the sandbox refuses to hand to DuckDB at
// all: over the byte cap or over the lexical nesting depth. The message
// names the exact limit tripped, so the HTTP layer can answer 400 with it.
var errSQLText = errors.New("query: sql: query text rejected")

// The query text budget: DuckDB sees no user text over the cap or deeper
// than the lexical depth, so parse and plan cost is bounded before any
// resource is touched. The depth limit mirrors DuckDB's own
// max_expression_depth (also pinned in applyLimits as the secondary guard).
const (
	sqlMaxQueryBytes = 64 << 10 // 64 KiB
	sqlMaxLexDepth   = 1000
)

// sqlPreflight rejects query text before DuckDB, the catalog or the object
// cache ever see it. It runs at the executor's Query entry and again at the
// sandbox's validate entry (for direct sandbox callers). It checks shape (the
// text cap and lexical depth) and one content policy that must precede the
// parser: an explicit WITH RECURSIVE pair. Recursive CTEs are not supported; a
// recursive CTE named logs would also slip past the AST relation policy, which
// always admits the logs name, so the ban cannot depend on the CTE name or on
// scope walking. The scanner fails open on unterminated strings or comments:
// the parser remains the authority on malformed text, which preflight lets
// through so it can fail with the real error class.
func sqlPreflight(query string) error {
	if n := len(query); n > sqlMaxQueryBytes {
		return fmt.Errorf("%w: %d bytes exceeds the %d-byte SQL text cap", errSQLText, n, sqlMaxQueryBytes)
	}
	if d := sqlLexDepth(query); d > sqlMaxLexDepth {
		return fmt.Errorf("%w: nesting depth %d exceeds the maximum of %d (parentheses, brackets and CASE)", errSQLText, d, sqlMaxLexDepth)
	}
	if sqlHasExplicitRecursive(query) {
		return fmt.Errorf("%w: %w", errSQLStatement, errSQLRecursive)
	}
	return nil
}

// sqlHasExplicitRecursive reports whether query contains the adjacent WITH
// RECURSIVE token pair outside strings, quoted identifiers, dollar-quoted
// strings and comments. The shared scanner applies DuckDB's lexical rules
// (case-insensitive words, comments skipped, including the Unicode spaces the
// parser normalizes to ASCII spaces before tokenizing), so this runs safely
// before the parser. Punctuation resets the window, so only two adjacent word
// tokens match.
//
// Pinned source evidence (d54cbbe3): the active parser is PEG
// (src/parser/parser.cpp:281-370); its grammar has
// `WithClause <- 'WITH' Recursive? List(WithStatement)` and
// `Recursive <- 'RECURSIVE'` (src/parser/peg/grammar/statements/select.gram:40-41),
// with `WithStatement <- ColIdOrString ColumnAliases? UsingKey? 'AS'
// Materialized? CTEBody` (:34) and `UsingKey <- 'USING' 'KEY'
// Parens(TargetList)` (:38). The transformer reads is_recursive from the
// optional RECURSIVE parse result and only then calls ToRecursiveCTE
// (src/parser/peg/transformer/transform_select.cpp:1054-1078). USING KEY is
// copied into the CTE entry's key_targets and is passed to ToRecursiveCTE
// only inside that branch (same file:1080-1092); a non-RECURSIVE USING KEY CTE
// is not recursive and fails the binder with "Circular reference ... use WITH
// RECURSIVE" (bounded probe, 5-row query).
func sqlHasExplicitRecursive(query string) bool {
	s := sqlScanner{q: query}
	prevWith := false
	for {
		word, punct := s.next()
		if word == "" && punct == 0 {
			return false
		}
		if word == "recursive" && prevWith {
			return true
		}
		prevWith = word == "with"
	}
}

// sqlScanner lexes SQL text for preflight and the keyword policy: it yields
// identifier words (lowercased, with $ and non-ASCII bytes as continuation
// characters the way DuckDB lexes identifiers) and single punctuation bytes,
// while consuming strings, quoted identifiers, dollar-quoted strings and
// comments. The lexical rules follow the pinned DuckDB parser, probe-verified
// on this build:
//
//   - Whitespace is ASCII space, tab, LF, CR, VT and FF, plus the Unicode
//     spaces Parser::StripUnicodeSpaces normalizes to ASCII spaces before
//     tokenizing (skipUnicodeSpace).
//
//   - Single-quoted strings double the quote character to stay open; an
//     E-prefixed string (the lone word e or E before a quote) also treats a
//     backslash before a quote as an escape, so E'a\' b' is one string.
//
//   - Double-quoted and backtick-quoted identifiers double their quote.
//
//   - A dollar quote opens with $tag$, where the tag starts with a letter,
//     underscore or non-ASCII byte (DuckDB lexes Unicode identifiers) and
//     continues with those plus digits (the empty tag $$ included); a $ that
//     directly continues an identifier (foo$bar is one identifier) never
//     opens a quote, and $1-style parameters are not quotes.
//
//   - Block comments nest, so everything up to the matching outer close is
//     comment; line comments run to the next LF or CR, DuckDB's two line
//     terminators (VT, FF and Unicode separators stay inside the comment).
//
// When a form is ambiguous the scanner prefers the reading that counts more
// structure, so its callers fail closed rather than letting nesting through.
// Unterminated strings and comments stop the scan; the parser remains the
// authority on malformed text.
type sqlScanner struct {
	q string
	i int
}

// skipUnicodeSpace consumes one Unicode space character that DuckDB replaces
// with an ASCII space before tokenizing, reporting whether it did. The pinned
// Parser::StripUnicodeSpaces (src/parser/parser.cpp:94-200) normalizes
// U+00A0, U+2000-U+200B, U+202F, U+205F, U+2060, U+3000 and U+FEFF outside
// quoted strings, quoted identifiers and dollar-quoted strings. The scanner
// has already consumed those constructs, so skipping the same characters here
// yields the token stream the parser sees, and the normalization also splits
// identifiers, so an identifier scan stops at one (sqlUnicodeSpaceLen).
func (s *sqlScanner) skipUnicodeSpace() bool {
	size := sqlUnicodeSpaceLen(s.q[s.i:])
	if size == 0 {
		return false
	}
	s.i += size
	return true
}

// sqlUnicodeSpaceLen returns the byte length of the Unicode space character
// that starts s, or zero when s does not start with one. The set is the one
// Parser::StripUnicodeSpaces replaces with an ASCII space before tokenizing.
func sqlUnicodeSpaceLen(s string) int {
	r, size := utf8.DecodeRuneInString(s)
	switch {
	case r == '\u00a0', r >= '\u2000' && r <= '\u200b',
		r == '\u202f', r == '\u205f', r == '\u2060', r == '\u3000', r == '\ufeff':
		return size
	}
	return 0
}

// next returns the next significant token: a lowercased identifier word
// (word != "") or a punctuation byte (punct != 0). At end of input both are
// zero. Skipped constructs produce no token.
func (s *sqlScanner) next() (word string, punct byte) {
	q, n := s.q, len(s.q)
	for s.i < n {
		c := q[s.i]
		if c >= utf8.RuneSelf && s.skipUnicodeSpace() {
			continue
		}
		switch {
		case c == ' ' || c == '\t' || c == '\r' || c == '\n' || c == '\v' || c == '\f':
			s.i++
		case c == '-' && s.i+1 < n && q[s.i+1] == '-':
			for s.i < n && q[s.i] != '\n' && q[s.i] != '\r' {
				s.i++
			}
		case c == '/' && s.i+1 < n && q[s.i+1] == '*':
			s.skipBlockComment()
		case c == '\'' || c == '"' || c == '`':
			s.skipQuoted(c, false)
		case c == '$':
			if s.skipDollarQuote() {
				continue
			}
			s.i++
			return "", '$'
		case sqlIdentStart(c):
			start := s.i
			for s.i < n && sqlIdentCont(q[s.i]) {
				if q[s.i] >= utf8.RuneSelf && sqlUnicodeSpaceLen(q[s.i:]) > 0 {
					break // the parser normalizes it to a separator
				}
				s.i++
			}
			w := strings.ToLower(q[start:s.i])
			if (w == "e") && s.i < n && q[s.i] == '\'' {
				s.skipQuoted('\'', true) // E-string: backslash escapes
				continue
			}
			return w, 0
		default:
			s.i++
			return "", c
		}
	}
	return "", 0
}

// skipBlockComment consumes one block comment including its nested inner
// comments; on an unterminated comment it consumes the rest of the input.
func (s *sqlScanner) skipBlockComment() {
	q, n := s.q, len(s.q)
	s.i += 2
	nested := 1
	for s.i < n && nested > 0 {
		switch {
		case q[s.i] == '/' && s.i+1 < n && q[s.i+1] == '*':
			nested++
			s.i += 2
		case q[s.i] == '*' && s.i+1 < n && q[s.i+1] == '/':
			nested--
			s.i += 2
		default:
			s.i++
		}
	}
}

// skipQuoted consumes one quoted construct opened by quote: a string or
// quoted identifier, where a doubled quote stays inside. escapes marks
// E-strings, whose backslash also escapes the quote.
func (s *sqlScanner) skipQuoted(quote byte, escapes bool) {
	q, n := s.q, len(s.q)
	s.i++ // opening quote
	for s.i < n {
		c := q[s.i]
		switch {
		case escapes && c == '\\':
			s.i += 2 // escaped byte stays inside
		case c == quote:
			if s.i+1 < n && q[s.i+1] == quote {
				s.i += 2 // doubled quote stays inside
				continue
			}
			s.i++
			return
		default:
			s.i++
		}
	}
}

// skipDollarQuote consumes a $tag$...$tag$ string when the input at the
// current position opens one, reporting whether it did. The tag starts with a
// letter, underscore or non-ASCII byte and continues with those plus digits,
// matching DuckDB's identifier rules (probe-verified with $é$, $aä1$ and
// $日本語$; a digit cannot start a tag, so $1$ is a parameter, and a bare
// dollar is not a quote — both are left for the caller).
func (s *sqlScanner) skipDollarQuote() bool {
	q, n := s.q, len(s.q)
	j := s.i + 1
	if j >= n {
		return false
	}
	if q[j] == '$' { // empty tag: $$ opens immediately
		return s.skipToDollarClose("$$")
	}
	if !sqlIdentStart(q[j]) {
		return false // $1-style parameter or bare dollar
	}
	for j < n && sqlDollarTagCont(q[j]) {
		j++
	}
	if j >= n || q[j] != '$' {
		return false
	}
	return s.skipToDollarClose(q[s.i : j+1])
}

// sqlDollarTagCont reports whether c continues a dollar-quote tag: every
// Unicode identifier continuation plus digits, but not '$', which ends it.
func sqlDollarTagCont(c byte) bool {
	return sqlIdentStart(c) || c >= '0' && c <= '9'
}

// skipToDollarClose consumes through the closing occurrence of tag.
func (s *sqlScanner) skipToDollarClose(tag string) bool {
	q, n := s.q, len(s.q)
	k := s.i + len(tag)
	for k < n {
		if q[k] == '$' && strings.HasPrefix(q[k:], tag) {
			s.i = k + len(tag)
			return true
		}
		k++
	}
	s.i = n // unterminated: stop, the parser reports it
	return true
}

// sqlIdentStart reports whether c may start an unquoted identifier: a
// letter, an underscore, or any non-ASCII byte (DuckDB lexes unicode
// identifiers).
func sqlIdentStart(c byte) bool {
	return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= 0x80
}

// sqlIdentCont reports whether c continues an unquoted identifier: start
// characters plus digits and the dollar sign (foo$bar is one identifier, so
// a $ that continues an identifier never opens a dollar quote).
func sqlIdentCont(c byte) bool {
	return sqlIdentStart(c) || c >= '0' && c <= '9' || c == '$'
}

// sqlLexDepth returns the maximum lexical nesting depth of query: one level
// per '(', '[' and CASE keyword, closed by ')', ']' and END, counted only
// outside strings, quoted identifiers, dollar-quoted strings and comments
// (the sqlScanner rules). Mismatched closers clamp at zero.
func sqlLexDepth(q string) int {
	maxDepth, depth := 0, 0
	s := sqlScanner{q: q}
	for {
		word, punct := s.next()
		if word == "" && punct == 0 {
			return maxDepth
		}
		switch {
		case punct == '(' || punct == '[' || word == "case":
			depth++
			maxDepth = max(maxDepth, depth)
		case punct == ')' || punct == ']' || word == "end":
			depth = max(depth-1, 0)
		}
	}
}

// sqlMaxASTNodes bounds the validation walk. A capped query cannot produce
// more nodes than this, so it is a safety net; the walk itself is linear.
const sqlMaxASTNodes = 250_000

// sqlValidateAST enforces the content policy on one serialized SELECT
// statement (the output of json_serialize_sql) in a single bounded, scoped
// traversal: the only named relations are the materialized logs table and
// CTEs actually visible at the reference's lexical scope, and no function
// from the settings/introspection/indirection or file-reading classes
// appears anywhere in the tree. Names are compared case-insensitively;
// quoting was already resolved by the parser, and any schema- or
// catalog-qualified relation or function is rejected outright.
func sqlValidateAST(ctx context.Context, stmt map[string]any) error {
	return sqlWalkScoped(ctx, stmt, nil, new(sqlASTWalk))
}

// sqlASTWalk counts visited nodes and checks the context, keeping the
// traversal linear and cancellable.
type sqlASTWalk struct {
	nodes int
}

// step admits one node, rejecting oversized trees and observing cancellation
// every 1024 nodes.
func (w *sqlASTWalk) step(ctx context.Context) error {
	w.nodes++
	if w.nodes > sqlMaxASTNodes {
		return fmt.Errorf("%w: query is too complex to validate (over %d AST nodes)", errSQLStatement, sqlMaxASTNodes)
	}
	if w.nodes&1023 == 0 {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("query: sql: validate: %w", err)
		}
	}
	return nil
}

// sqlWalkScoped walks the serialized statement once with a lexical scope
// stack of visible CTE names. A node carrying a cte_map is a query node
// (sqlWalkQueryNode): its CTE declarations are visible to its own references
// and everything nested below, and to nothing else — a name declared inside
// a subquery never authorizes a reference outside it. Each CTE body is
// validated with its outer scopes plus the siblings declared before it in
// the same WITH: its own name and later siblings are not visible, and
// self-reference is rejected outright: without RECURSIVE it is a scope
// violation. Explicit WITH RECURSIVE normally never reaches this walk because
// sqlPreflight rejects the pair first; as a second layer, any serialized node
// whose type is RECURSIVE_CTE_NODE is rejected here too, should scanner or
// normalization handling ever miss a spelling (the json_serialize_sql output
// carries that node type even though it has no separate RECURSIVE marker).
// Recursive CTEs are a documented unsupported feature (fail closed).
func sqlWalkScoped(ctx context.Context, node any, scopes []map[string]bool, w *sqlASTWalk) error {
	switch n := node.(type) {
	case map[string]any:
		if err := w.step(ctx); err != nil {
			return err
		}
		if n["type"] == "RECURSIVE_CTE_NODE" {
			return fmt.Errorf("%w: %w", errSQLStatement, errSQLRecursive)
		}
		if cm, ok := n["cte_map"].(map[string]any); ok {
			return sqlWalkQueryNode(ctx, n, cm, scopes, w)
		}
		if err := sqlCheckTableNode(n, scopes); err != nil {
			return err
		}
		if err := sqlCheckFunctionNode(n); err != nil {
			return err
		}
		keys := slices.Sorted(maps.Keys(n))
		for _, k := range keys {
			if err := sqlWalkScoped(ctx, n[k], scopes, w); err != nil {
				return err
			}
		}
	case []any:
		if err := w.step(ctx); err != nil {
			return err
		}
		for _, v := range n {
			if err := sqlWalkScoped(ctx, v, scopes, w); err != nil {
				return err
			}
		}
	}
	return nil
}

// sqlWalkQueryNode walks one query node: every CTE entry first, under outer
// scopes plus its prior siblings only, then the node's remaining children
// (select list, from, where, subqueries and set-operation branches) under
// outer scopes plus the node's complete declaration set.
func sqlWalkQueryNode(ctx context.Context, n, cm map[string]any, scopes []map[string]bool, w *sqlASTWalk) error {
	entries := sqlCTEEntries(cm)
	own := make(map[string]bool, len(entries))
	for _, e := range entries {
		if key, ok := e["key"].(string); ok {
			own[strings.ToLower(key)] = true
		}
	}
	prior := make(map[string]bool, len(entries))
	for _, e := range entries {
		if err := sqlWalkScoped(ctx, e, sqlChildScopes(scopes, prior), w); err != nil {
			return err
		}
		if key, ok := e["key"].(string); ok {
			prior[strings.ToLower(key)] = true
		}
	}
	inner := sqlChildScopes(scopes, own)
	for _, k := range slices.Sorted(maps.Keys(n)) {
		if k == "cte_map" {
			continue
		}
		if err := sqlWalkScoped(ctx, n[k], inner, w); err != nil {
			return err
		}
	}
	return nil
}

// sqlCTEEntries returns a cte_map's entry objects in declaration order (the
// serialized array preserves it).
func sqlCTEEntries(cm map[string]any) []map[string]any {
	list, _ := cm["map"].([]any)
	entries := make([]map[string]any, 0, len(list))
	for _, e := range list {
		if m, ok := e.(map[string]any); ok {
			entries = append(entries, m)
		}
	}
	return entries
}

// sqlChildScopes extends scopes with one more declaration set, copying the
// stack so sibling walks never observe each other's additions.
func sqlChildScopes(scopes []map[string]bool, own map[string]bool) []map[string]bool {
	child := make([]map[string]bool, len(scopes)+1)
	copy(child, scopes)
	child[len(scopes)] = own
	return child
}

// sqlScopeHas reports whether name is declared by any enclosing scope.
func sqlScopeHas(scopes []map[string]bool, name string) bool {
	for _, s := range scopes {
		if s[name] {
			return true
		}
	}
	return false
}

// sqlCheckTableNode enforces the relation policy on one BASE_TABLE node.
// CTE references serialize as BASE_TABLE too, with the CTE's name and no
// schema; the parser already resolved quoting and preserves case, which the
// case-insensitive comparison handles.
func sqlCheckTableNode(n map[string]any, scopes []map[string]bool) error {
	if n["type"] != "BASE_TABLE" {
		return nil
	}
	table, _ := n["table_name"].(string)
	schema, _ := n["schema_name"].(string)
	catalog, _ := n["catalog_name"].(string)
	qualified := schema != "" || catalog != ""
	if !qualified {
		if path, ok := sqlASTPath(n); ok && len(path) > 1 {
			qualified = true
			table = strings.Join(path, ".")
		}
	}
	if qualified {
		return fmt.Errorf("%w: qualified relation %q is not allowed; the SQL API exposes only logs", errSQLStatement, catalog+"."+schema+"."+table)
	}
	if name := strings.ToLower(table); name != "logs" && !sqlScopeHas(scopes, name) {
		return fmt.Errorf("%w: relation %q is not the logs relation or a CTE visible at this scope", errSQLStatement, table)
	}
	return nil
}

// sqlCheckFunctionNode enforces the function policy on one function node:
// scalar, aggregate and window functions carry function_name directly, and
// table functions carry their description under "function".
func sqlCheckFunctionNode(n map[string]any) error {
	if fn, ok := n["function"].(map[string]any); ok {
		if err := sqlCheckOneFunction(fn); err != nil {
			return err
		}
	}
	if _, ok := n["function_name"]; ok {
		return sqlCheckOneFunction(n)
	}
	return nil
}

// sqlCheckOneFunction rejects one function call: any schema- or
// catalog-qualified call, the settings/env introspection and indirection
// names, and the file-reading table functions. Everything else — the
// ordinary scalar and aggregate analytics — stays available; the sandbox's
// filesystem and configuration locks back the whole policy regardless.
func sqlCheckOneFunction(fn map[string]any) error {
	schema, _ := fn["schema"].(string)
	catalog, _ := fn["catalog"].(string)
	if schema == "" && catalog == "" {
		if path, ok := sqlASTPath(fn); ok && len(path) > 1 {
			schema = strings.Join(path[:len(path)-1], ".")
		}
	}
	if schema != "" || catalog != "" {
		return fmt.Errorf("%w: qualified function %q is not allowed", errSQLStatement, catalog+"."+schema)
	}
	name, _ := fn["function_name"].(string)
	if name == "" {
		if path, ok := sqlASTPath(fn); ok && len(path) > 0 {
			name = path[len(path)-1]
		}
	}
	if name == "" {
		return nil
	}
	low := strings.ToLower(name)
	for _, prefix := range []string{"duckdb_", "pragma_", "sqlite_", "read_"} {
		if strings.HasPrefix(low, prefix) {
			return fmt.Errorf("%w: function %q introspects or reads outside the logs relation", errSQLStatement, name)
		}
	}
	switch low {
	case "current_setting", "getvariable", "getenv",
		"query", "query_table", "json_execute_serialized_sql",
		"glob", "glob_parquet", "parquet_scan", "parquet_metadata",
		"parquet_schema", "parquet_kv_metadata", "sniff_csv":
		return fmt.Errorf("%w: function %q introspects or reads outside the logs relation", errSQLStatement, name)
	}
	return nil
}

// sqlASTPath returns the qualified_name path of a node, when present.
func sqlASTPath(n map[string]any) ([]string, bool) {
	qn, ok := n["qualified_name"].(map[string]any)
	if !ok {
		return nil, false
	}
	elems, ok := qn["path"].([]any)
	if !ok {
		return nil, false
	}
	path := make([]string, len(elems))
	for i, e := range elems {
		s, ok := e.(string)
		if !ok {
			return nil, false
		}
		path[i] = s
	}
	return path, true
}

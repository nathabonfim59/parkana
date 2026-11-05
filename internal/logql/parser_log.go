// The log side of the A2 grammar: log queries (a selector and its pipeline),
// log ranges (a pipeline closed by [range] and offset, in every placement
// Loki's logRangeExpr rule allows, syntax.y lines 128-153), and the pipeline
// stages — line filters with or-alternatives, the json/logfmt/regexp/pattern
// parsers, label filters, line_format, label_format, drop, keep and unwrap.
//
// Pipeline-stage boundaries follow the pinned grammar exactly: a stage is
// either a run of juxtaposed line filters (no leading pipe) or one piped
// stage; after a piped label-filter stage a bare line-filter operator starts
// a new juxtaposed run ({a="b"} | foo="x" != "y" is valid), but a label
// filter always needs its pipe. The | unwrap stage only exists inside a log
// range: reaching one without a following range is a syntax error.

package logql

import (
	"strconv"
	"strings"
	"time"

	"github.com/nathabonfim59/parkana/internal/plan"
)

// logUnit is a selector with its pipeline stages and optional unwrap stage,
// not yet closed by a range: the grammar's selector [pipelineExpr]
// [unwrapExpr] prefix of every logRangeExpr production. paren records that
// the unit's range attaches after a closing parenthesis, where the grammar
// allows no post-range pipeline (syntax.y lines 131-148): stages exist
// only in the bare-selector productions (lines 149-152), though a
// parenthesized bare selector still takes a post-range unwrap
// (lines 135-136).
type logUnit struct {
	q     *LogQuery
	uw    *Unwrap // nil without a | unwrap stage
	paren bool    // unit's range attached after a closing ')'
}

// parseLogUnit parses selector [pipeline stages] [| unwrap ...]. The cursor
// must be at '{'.
func (p *parser) parseLogUnit() (*logUnit, error) {
	sel, err := parseSelector(p)
	if err != nil {
		return nil, err
	}
	u := &logUnit{q: &LogQuery{Pos: sel.Pos, Selector: *sel}}
	u.q.Stages, err = p.parseStages()
	if err != nil {
		return nil, err
	}
	if p.atUnwrap() {
		p.next() // '|'
		p.next() // 'unwrap'
		if u.uw, err = p.parseUnwrap(); err != nil {
			return nil, err
		}
	}
	return u, nil
}

// atUnwrap reports whether the cursor is at a | unwrap that belongs to the
// enclosing log range.
func (p *parser) atUnwrap() bool {
	return p.cur().Kind == tokPipe && identIs(p.peek(1), "unwrap")
}

// parseStages parses pipeline stages until a token that cannot continue a
// pipeline: juxtaposed line-filter runs and piped stages. It stops before a
// | unwrap (the caller owns it) and never consumes a range or offset.
func (p *parser) parseStages() ([]Stage, error) {
	var stages []Stage
	for {
		var (
			st  Stage
			err error
		)
		switch p.cur().Kind {
		case tokPipeEq, tokNeq, tokPipeRe, tokNre, tokPipePattern, tokNotPattern:
			st, err = p.parseLineFilter()
		case tokPipe:
			if p.atUnwrap() {
				return stages, nil
			}
			st, err = p.parsePipedStage()
		default:
			return stages, nil
		}
		if err != nil {
			return nil, err
		}
		stages = append(stages, st)
	}
}

// parseLineFilter parses one juxtaposed line-filter run: an operator and a
// quoted value with its or-alternatives, as in Loki's lineFilter/orFilter
// rules. Values keep the surface OR syntax (the evaluator applies
// newOrLineFilterExpr semantics: |= and |~ chains are unions, != and !~
// chains negated unions, i.e. intersections). The |> and !> pattern line
// filters and ip() filter operands are valid Loki but deferred families:
// their full syntax validates here, then the whole filter defers.
func (p *parser) parseLineFilter() (Stage, error) {
	opTok := p.next()
	var op plan.LineFilterType
	switch opTok.Kind {
	case tokPipeEq:
		op = plan.LineContains
	case tokPipeRe:
		op = plan.LineRegexp
	case tokNeq:
		op = plan.LineNotContains
	case tokNre:
		op = plan.LineNotRegexp
	case tokPipePattern, tokNotPattern:
		op = plan.LineContains // placeholder; the stage defers below
	default:
		return nil, badQuery(opTok.Pos, "expected line filter operator, found %s", opTok)
	}
	lf := &LineFilter{Pos: opTok.Pos, Op: op}
	var b strings.Builder
	b.WriteString(opTok.Text)
	deferred := opTok.Kind == tokPipePattern || opTok.Kind == tokNotPattern
	for {
		if p.atIPFilter() {
			addr, err := p.parseIPFilter()
			if err != nil {
				return nil, err
			}
			b.WriteString(` ip(` + strconv.Quote(addr) + `)`)
			deferred = true
		} else {
			v, err := p.expect(tokString, "line filter")
			if err != nil {
				return nil, err
			}
			lf.Values = append(lf.Values, v.Str)
			b.WriteString(" " + strconv.Quote(v.Str))
		}
		if !identIs(p.cur(), "or") {
			break
		}
		p.next()
		b.WriteString(" or")
	}
	if deferred {
		p.deferUnsupported(opTok.Pos, "pattern/ip line filters are not supported")
		return &deferredStage{pos: opTok.Pos, what: b.String()}, nil
	}
	return lf, nil
}

// atIPFilter reports whether the cursor is at ip( — a Loki filter function.
func (p *parser) atIPFilter() bool {
	return p.cur().Kind == tokIdent && strings.EqualFold(p.cur().Text, "ip") && p.peek(1).Kind == tokLParen
}

// parseIPFilter consumes ip("..."), a valid Loki filter function Parkana
// defers, and returns the validated address string.
func (p *parser) parseIPFilter() (string, error) {
	ipTok := p.next() // ip
	if _, err := p.expect(tokLParen, "ip filter"); err != nil {
		return "", err
	}
	addr, err := p.expect(tokString, "ip filter")
	if err != nil {
		return "", err
	}
	if _, err := p.expect(tokRParen, "ip filter"); err != nil {
		return "", err
	}
	p.deferUnsupported(ipTok.Pos, "ip() filters are not supported")
	return addr.Str, nil
}

// parsePipedStage parses the stage after a '|'. The cursor is at '|'. A
// piped stage is a named stage or a label filter — which may itself start
// with '(' (labelFilter: OPEN_PARENTHESIS labelFilter CLOSE_PARENTHESIS).
func (p *parser) parsePipedStage() (Stage, error) {
	pipe := p.next()
	if p.cur().Kind == tokLParen {
		f, err := p.parseLabelFilterExpr(1)
		if err != nil {
			return nil, err
		}
		return f.(Stage), nil
	}
	name := p.cur()
	if name.Kind != tokIdent {
		return nil, badQuery(name.Pos, "expected pipeline stage after '|', found %s", name)
	}
	switch strings.ToLower(name.Text) {
	case "json":
		p.next()
		ext, err := p.parseExtractions("json")
		if err != nil {
			return nil, err
		}
		return &JSONParser{Pos: pipe.Pos, Extractions: ext}, nil
	case "logfmt":
		p.next()
		var strict, keepEmpty bool
		for {
			flag, ok, err := p.parseParserFlag()
			if err != nil {
				return nil, err
			}
			if !ok {
				break
			}
			switch flag {
			case "--strict":
				strict = true
			case "--keep-empty":
				keepEmpty = true
			}
		}
		ext, err := p.parseExtractions("logfmt")
		if err != nil {
			return nil, err
		}
		return &LogfmtParser{Pos: pipe.Pos, Strict: strict, KeepEmpty: keepEmpty, Extractions: ext}, nil
	case "regexp":
		p.next()
		pat, err := p.expect(tokString, "regexp parser")
		if err != nil {
			return nil, err
		}
		rp, err := NewRegexpParser(pipe.Pos, pat.Str)
		if err != nil {
			return nil, err
		}
		return rp, nil
	case "pattern":
		p.next()
		pat, err := p.expect(tokString, "pattern parser")
		if err != nil {
			return nil, err
		}
		return &PatternParser{Pos: pipe.Pos, Pattern: pat.Str}, nil
	case "line_format":
		p.next()
		tmpl, err := p.expect(tokString, "line_format")
		if err != nil {
			return nil, err
		}
		return &LineFormat{Pos: pipe.Pos, Template: tmpl.Str}, nil
	case "label_format":
		p.next()
		return p.parseLabelFormat(pipe.Pos)
	case "drop", "keep":
		p.next()
		targets, err := p.parseDropTargets(name.Text)
		if err != nil {
			return nil, err
		}
		if strings.ToLower(name.Text) == "drop" {
			return &Drop{Pos: pipe.Pos, Targets: targets}, nil
		}
		return &Keep{Pos: pipe.Pos, Targets: targets}, nil
	case "unpack":
		p.next()
		p.deferUnsupported(pipe.Pos, "| unpack is not supported")
		return &deferredStage{pos: pipe.Pos, what: "unpack"}, nil
	case "decolorize":
		p.next()
		p.deferUnsupported(pipe.Pos, "| decolorize is not supported")
		return &deferredStage{pos: pipe.Pos, what: "decolorize"}, nil
	default:
		if reservedWord(name.Text) {
			return nil, badQuery(name.Pos, "%s is a reserved word and cannot start a pipeline stage", name.Text)
		}
		f, err := p.parseLabelFilterExpr(1)
		if err != nil {
			return nil, err
		}
		return f.(Stage), nil // leaf, binary and deferred filters are stages
	}
}

// parseParserFlag consumes one --strict/--keep-empty flag if present.
// Flags are spelled exactly (Loki's tryScanFlag matches the parserFlags map
// without lowering), so --STRICT is not a flag and is rejected. It returns
// ok=false without consuming when no flag follows.
func (p *parser) parseParserFlag() (flag string, ok bool, err error) {
	if p.cur().Kind != tokMinus {
		return "", false, nil
	}
	// Rebuild the candidate from adjacent '-' and ident tokens: the lexer
	// has no flag token, and Loki's scanner consumes letter/dash runs
	// starting with '--'.
	start := p.cur()
	end := p.i + 1 // the leading '-' is part of any flag
	for end < len(p.toks) {
		t := p.toks[end]
		if t.Kind != tokMinus && t.Kind != tokIdent {
			break
		}
		if !adjacent(p.toks[end-1], t) {
			break
		}
		end++
	}
	text := tokenText(p.toks[p.i:end])
	switch text {
	case "--strict", "--keep-empty":
	default:
		return "", false, badQuery(start.Pos, "invalid logfmt flag %s (expected --strict or --keep-empty)", strconv.Quote(text))
	}
	p.i = end
	return text, true, nil
}

// tokenText concatenates the text of tokens; used by flag reconstruction.
func tokenText(ts []token) string {
	var b strings.Builder
	for _, t := range ts {
		b.WriteString(t.Text)
	}
	return b.String()
}

// parseExtractions parses [name="path" | name, ...]: Loki's
// labelExtractionExpressionList, where a bare name extracts the same-name
// field. No trailing comma.
func (p *parser) parseExtractions(parser string) ([]NamedExtraction, error) {
	var ext []NamedExtraction
	if !isLabelName(p.cur()) {
		return nil, nil
	}
	for {
		name, err := p.expectLabelName(parser + " extraction")
		if err != nil {
			return nil, err
		}
		e := NamedExtraction{Name: name.Text, Path: name.Text}
		if p.cur().Kind == tokAssign {
			p.next()
			path, err := p.expect(tokString, parser+" extraction")
			if err != nil {
				return nil, err
			}
			e.Path = path.Str
		}
		ext = append(ext, e)
		if p.cur().Kind != tokComma {
			return ext, nil
		}
		p.next()
	}
}

// parseLabelFormat parses comma-separated dest=src renames and dest="tmpl"
// template assignments. An empty template is a template, not a rename.
func (p *parser) parseLabelFormat(pos Position) (Stage, error) {
	f := &LabelFormat{Pos: pos}
	for {
		dest, err := p.expectLabelName("label_format destination")
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tokAssign, "label_format"); err != nil {
			return nil, err
		}
		switch p.cur().Kind {
		case tokIdent:
			src, err := p.expectLabelName("label_format source")
			if err != nil {
				return nil, err
			}
			f.Replacements = append(f.Replacements, LabelReplacement{Dest: dest.Text, Src: src.Text})
		case tokString:
			tmpl := p.next()
			f.Replacements = append(f.Replacements, LabelReplacement{Dest: dest.Text, IsTemplate: true, Template: tmpl.Str})
		default:
			return nil, badQuery(p.cur().Pos, "expected label name or quoted template in label_format, found %s", p.cur())
		}
		if p.cur().Kind != tokComma {
			return f, nil
		}
		p.next()
	}
}

// parseDropTargets parses drop/keep targets: label names and string
// matchers, comma-separated (Loki's namedMatchers rule).
func (p *parser) parseDropTargets(stage string) ([]DropTarget, error) {
	var targets []DropTarget
	for {
		name, err := p.expectLabelName(stage)
		if err != nil {
			return nil, err
		}
		t := DropTarget{Name: name.Text}
		switch p.cur().Kind {
		case tokAssign, tokNeq, tokRe, tokNre:
			op := p.next()
			val, err := p.expect(tokString, stage)
			if err != nil {
				return nil, err
			}
			var typ plan.MatchType
			switch op.Kind {
			case tokAssign:
				typ = plan.MatchEqual
			case tokNeq:
				typ = plan.MatchNotEqual
			case tokRe:
				typ = plan.MatchRegexp
			default:
				typ = plan.MatchNotRegexp
			}
			m, err := plan.NewMatcher(typ, name.Text, labelMatcherValue(typ, val.Str))
			if err != nil {
				return nil, badQuery(op.Pos, "invalid %s target: %v", stage, err)
			}
			t = DropTarget{Matcher: &m}
		}
		targets = append(targets, t)
		if p.cur().Kind != tokComma {
			return targets, nil
		}
		p.next()
	}
}

// parseUnwrap parses the unwrap stage after its keyword: a label, or a
// conversion function applied to one, followed by any number of post label
// filters (syntax.y unwrapExpr).
func (p *parser) parseUnwrap() (*Unwrap, error) {
	kw := p.cur()
	label, err := p.expectLabelName("unwrap")
	if err != nil {
		return nil, err
	}
	u := &Unwrap{Pos: kw.Pos, Label: label.Text}
	if p.cur().Kind == tokLParen && isConversion(label.Text) && p.looksLikeFunction(p.i-1) {
		// duration(label), bytes(label), duration_seconds(label): the
		// conversion keyword is only a function token when followed by '('.
		p.next()
		arg, err := p.expectLabelName("unwrap conversion")
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tokRParen, "unwrap conversion"); err != nil {
			return nil, err
		}
		u.Func = conversionFunc(label.Text)
		u.Label = arg.Text
	}
	for p.cur().Kind == tokPipe && labelFilterStarts(p.peek(1)) {
		p.next()
		f, err := p.parseLabelFilterExpr(1)
		if err != nil {
			return nil, err
		}
		u.PostFilters = append(u.PostFilters, f)
	}
	return u, nil
}

// isConversion reports whether name is one of Loki's unwrap conversion
// functions (convOp: duration, bytes, duration_seconds).
func isConversion(name string) bool {
	switch strings.ToLower(name) {
	case "duration", "bytes", "duration_seconds":
		return true
	}
	return false
}

func conversionFunc(name string) UnwrapFunc {
	switch strings.ToLower(name) {
	case "duration":
		return UnwrapDuration
	case "bytes":
		return UnwrapBytes
	default:
		return UnwrapDurationSeconds
	}
}

// labelFilterStarts reports whether token t can begin a label-filter
// expression: a label name or '(' (syntax.y labelFilter).
func labelFilterStarts(t token) bool {
	return isLabelName(t) || t.Kind == tokLParen
}

// parseLabelFilterExpr parses a label-filter boolean expression with
// Loki's precedence: or loosest; and, comma and juxtaposition tighter and
// left-associative (syntax.y labelFilter rules, or < and/comma/implicit).
func (p *parser) parseLabelFilterExpr(minPrec int) (LabelFilterExpr, error) {
	if err := p.enter(p.cur().Pos); err != nil {
		return nil, err
	}
	defer p.leave()
	lhs, err := p.parseLabelFilterOperand()
	if err != nil {
		return nil, err
	}
	lhsDepth := filterDepth(lhs)
	for {
		var (
			op      BoolOp
			prec    int
			consume = true
		)
		t := p.cur()
		switch {
		case identIs(t, "or"):
			op, prec = OpFilterOr, 1
		case identIs(t, "and"):
			op, prec = OpFilterAnd, 2
		case t.Kind == tokComma:
			op, prec = OpComma, 2
		case labelFilterStarts(t):
			op, prec, consume = OpFilterAnd, 2, false // juxtaposition
		default:
			return lhs, nil
		}
		if prec < minPrec {
			return lhs, nil
		}
		if consume {
			p.next()
		}
		rhs, err := p.parseLabelFilterExpr(prec + 1)
		if err != nil {
			return nil, err
		}
		lhs = &LabelFilterBinary{Pos: t.Pos, Op: op, LHS: lhs, RHS: rhs}
		depth := 1 + max(lhsDepth, filterDepth(rhs))
		if depth > maxExprDepth {
			return nil, badQuery(t.Pos, "label filter nested deeper than %d levels", maxExprDepth)
		}
		lhsDepth = depth
	}
}

// parseLabelFilterOperand parses one label comparison or a parenthesized
// filter: label op value, where the value is a string (matcher), a number
// (with optional sign), a duration, a byte size or an ip() filter.
func (p *parser) parseLabelFilterOperand() (LabelFilterExpr, error) {
	if p.cur().Kind == tokLParen {
		if err := p.enter(p.cur().Pos); err != nil {
			return nil, err
		}
		p.next()
		f, err := p.parseLabelFilterExpr(1)
		if err != nil {
			return nil, err
		}
		p.leave()
		if _, err := p.expect(tokRParen, "label filter"); err != nil {
			return nil, err
		}
		return f, nil
	}
	label, err := p.expectLabelName("label filter")
	if err != nil {
		return nil, err
	}
	opTok := p.cur()
	var op LabelOp
	// == and = build the same filter for typed values (Loki's grammar maps
	// both EQ and CMP_EQ to LabelFilterEqual), but == rejects strings.
	eqeq := false
	switch opTok.Kind {
	case tokAssign:
		op = OpLabelEq
	case tokNeq:
		op = OpLabelNeq
	case tokEq:
		op, eqeq = OpLabelEq, true
	case tokRe:
		op = OpLabelRe
	case tokNre:
		op = OpLabelNre
	case tokGt:
		op = OpLabelGt
	case tokGte:
		op = OpLabelGte
	case tokLt:
		op = OpLabelLt
	case tokLte:
		op = OpLabelLte
	default:
		return nil, badQuery(opTok.Pos, "expected comparison operator after label %s, found %s", label.Text, opTok)
	}
	p.next()
	// ip() filter operand: valid Loki, deferred family — but the grammar's
	// ipLabelFilter rule takes EQ or NEQ only, so == is a syntax error.
	if p.atIPFilter() {
		if (op != OpLabelEq && op != OpLabelNeq) || eqeq {
			return nil, badQuery(opTok.Pos, "ip() filter needs = or !=, found %s", opTok)
		}
		addr, err := p.parseIPFilter()
		if err != nil {
			return nil, err
		}
		return &deferredFilter{pos: opTok.Pos, what: label.Text + " " + opTok.Text + ` ip(` + strconv.Quote(addr) + `)`}, nil
	}
	switch p.cur().Kind {
	case tokString:
		v := p.next()
		if eqeq {
			return nil, badQuery(opTok.Pos, `== needs a number, duration or bytes value; use = for strings`)
		}
		switch op {
		case OpLabelEq, OpLabelNeq, OpLabelRe, OpLabelNre:
		default:
			return nil, badQuery(opTok.Pos, "%s needs a number, duration or bytes value, found a string", op)
		}
		return NewLabelFilterLeaf(label.Pos, label.Text, op, FilterValue{Kind: StringValue, Str: v.Str})
	case tokNumber, tokPlus, tokMinus:
		// Loki's lexer glues '-' to a duration in any context (lex.go
		// lines 177-189), so -1s and -1.5s are single duration values here
		// while '- 1s' and '+1s' stay errors; bytes never take a sign.
		if p.cur().Kind == tokMinus && adjacent(p.cur(), p.peek(1)) && p.peek(1).Kind == tokDuration {
			p.next() // '-'
			d := p.next()
			return NewLabelFilterLeaf(label.Pos, label.Text, op, FilterValue{Kind: DurationValue, Dur: -d.Dur})
		}
		num, err := p.parseSignedNumber()
		if err != nil {
			return nil, err
		}
		return NewLabelFilterLeaf(label.Pos, label.Text, op, FilterValue{Kind: NumberValue, Num: num})
	case tokDuration:
		d := p.next()
		return NewLabelFilterLeaf(label.Pos, label.Text, op, FilterValue{Kind: DurationValue, Dur: d.Dur})
	case tokBytes:
		b := p.next()
		return NewLabelFilterLeaf(label.Pos, label.Text, op, FilterValue{Kind: BytesValue, Bytes: b.Bytes})
	default:
		return nil, badQuery(p.cur().Pos, "expected string, number, duration or bytes value after %s %s, found %s", label.Text, opTok, p.cur())
	}
}

// parseSignedNumber parses [+-]? NUMBER, Loki's literalExpr rule, which
// allows a sign directly before a number in any spacing position.
func (p *parser) parseSignedNumber() (float64, error) {
	neg := false
	switch p.cur().Kind {
	case tokPlus:
		p.next()
	case tokMinus:
		neg = true
		p.next()
	}
	t, err := p.expect(tokNumber, "number")
	if err != nil {
		return 0, err
	}
	if neg {
		return -t.Num, nil
	}
	return t.Num, nil
}

// parseOffset parses the offset keyword followed by a duration, which may be
// negative: Loki's lexer only produces a negative duration token for '-' and
// the unit glued together (lex.go lines 171-188), so 'offset - 5m' with a
// space is a syntax error while 'offset -5m' parses.
func (p *parser) parseOffset() (time.Duration, error) {
	kw := p.next() // 'offset'
	switch p.cur().Kind {
	case tokDuration:
		return p.next().Dur, nil
	case tokMinus:
		minus := p.next()
		d := p.cur()
		if d.Kind != tokDuration || !adjacent(minus, d) {
			return 0, badQuery(minus.Pos, "expected duration after 'offset -' without a space")
		}
		p.next()
		return -d.Dur, nil
	default:
		return 0, badQuery(kw.Pos, "expected duration after 'offset', found %s", p.cur())
	}
}

// closeRange closes a log unit with [range] [offset] [stages] [unwrap],
// merging the post-range pipeline into the query's stages. Post-range
// stages belong to the bare-selector productions only (syntax.y
// logRangeExpr lines 149-152): a unit that already has a pipeline or unwrap
// before the range, or whose range attaches after a closing parenthesis
// (lines 131-148), must stop at the offset. The one exception is a
// parenthesized bare selector, which still allows a post-range unwrap
// (lines 135-136). The cursor must be at '['.
func (p *parser) closeRange(u *logUnit) (*LogRange, error) {
	bracket := p.next() // '['
	r := &LogRange{Pos: bracket.Pos, Pipeline: u.q, Unwrap: u.uw}
	preStages := len(u.q.Stages)
	d, err := p.parseRangeDuration(bracket)
	if err != nil {
		return nil, err
	}
	r.Range = d
	if identIs(p.cur(), "offset") {
		r.Offset, err = p.parseOffset()
		if err != nil {
			return nil, err
		}
	}
	postStages, err := p.parseStages()
	if err != nil {
		return nil, err
	}
	postUnwrap := false
	if p.atUnwrap() {
		if u.uw != nil {
			return nil, badQuery(p.cur().Pos, "only one unwrap stage is allowed")
		}
		postUnwrap = true
	}
	// Post-range placements (syntax.y logRangeExpr): stages only in the
	// bare-selector productions 149-152; unwrap in those (134, 151-152)
	// and after a parenthesized bare selector (135-136). A pre-range
	// pipeline or unwrap, or a pipeline inside parentheses, always ends
	// the production at the offset (lines 131-148).
	selectorOnly := preStages == 0 && u.uw == nil
	if len(postStages) > 0 && (!selectorOnly || u.paren) {
		return nil, badQuery(bracket.Pos, "pipeline stages after the range need a bare selector before it")
	}
	if postUnwrap && !selectorOnly {
		return nil, badQuery(bracket.Pos, "unwrap after the range needs a selector with no pipeline or unwrap before it")
	}
	r.Pipeline.Stages = append(r.Pipeline.Stages, postStages...)
	if postUnwrap {
		p.next() // '|'
		p.next() // 'unwrap'
		if r.Unwrap, err = p.parseUnwrap(); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// parseRangeDuration parses the duration inside range brackets. Loki's lexer
// scans '[' to ']' as one token and parses the content with
// model.ParseDuration only (lex.go lines 206-223), so fractional or
// sub-millisecond forms accepted by the general duration token
// (time.ParseDuration fallback) are invalid here — 1.5s and 500ns are
// syntax errors in a range — and neither is whitespace or a comment between
// the brackets, because the scanned content must parse as a duration.
// model.ParseDuration also accepts exactly "0" (prometheus/common v0.67.5
// model/time.go lines 205-212), which lexes as a plain number here.
func (p *parser) parseRangeDuration(bracket token) (time.Duration, error) {
	t := p.cur()
	if t.Kind == tokNumber && t.Text == "0" {
		t.Dur = 0
	} else if t.Kind != tokDuration {
		return 0, badQuery(t.Pos, "expected duration in range, found %s", t)
	}
	if !adjacent(bracket, t) {
		return 0, badQuery(bracket.Pos, "no whitespace or comments are allowed inside range brackets")
	}
	if t.Kind == tokDuration {
		if _, ok := parseModelDuration(t.Text); !ok {
			return 0, badQuery(bracket.Pos, "invalid range duration %s: must be whole units of y w d h m s ms", strconv.Quote(t.Text))
		}
	}
	p.next()
	closing := p.cur()
	if closing.Kind != tokRBracket || !adjacent(t, closing) {
		return 0, badQuery(closing.Pos, "expected ']' immediately after the range duration, found %s", closing)
	}
	p.next()
	return t.Dur, nil
}

// parseLogParen consumes one parenthesis layer around log content plus
// everything the grammar allows immediately inside and after it, following
// the parenthesized productions of logRangeExpr (syntax.y lines 131-153):
// '(' selector [pipeline] [unwrap] ')' [range ...] and '(' logRangeExpr ')'
// recursively. A unit parenthesized more than once cannot take a range —
// only the innermost parenthesis matches the '( selector pipelineExpr )'
// productions.
func (p *parser) parseLogParen() (Expr, *logUnit, bool, error) {
	open := p.next() // '('
	if err := p.enter(open.Pos); err != nil {
		return nil, nil, false, err
	}
	defer p.leave()
	var (
		expr     Expr
		unit     *logUnit
		complete bool
	)
	switch p.cur().Kind {
	case tokLBrace:
		u, err := p.parseLogUnit()
		if err != nil {
			return nil, nil, false, err
		}
		if p.cur().Kind == tokLBracket {
			r, err := p.closeRange(u)
			if err != nil {
				return nil, nil, false, err
			}
			expr, complete = r, true
		} else {
			expr, unit = u.q, u
		}
	case tokLParen:
		inner, _, innerComplete, err := p.parseLogParen()
		if err != nil {
			return nil, nil, false, err
		}
		expr = inner
		complete = innerComplete
		// A unit already wrapped in one parenthesis layer is a logExpr in
		// the grammar, and only the '( selector pipelineExpr )' productions
		// take a range after ')': (({a="b"} | json))[5m] is invalid while
		// ({a="b"} | json)[5m] and ({a="b"} | json [5m]) are not. So a
		// nested paren's unit does not stay range-attachable here.
	default:
		return nil, nil, false, badQuery(p.cur().Pos, "expected selector after '(', found %s", p.cur())
	}
	if _, err := p.expect(tokRParen, "log query"); err != nil {
		return nil, nil, false, err
	}
	if unit != nil && unit.uw != nil && p.cur().Kind != tokLBracket {
		return nil, nil, false, badQuery(unit.uw.Pos, "unwrap requires a range")
	}
	if unit != nil && p.cur().Kind == tokLBracket {
		// The range attaches after ')', so the unit is one of the
		// '( selector ... )' productions (lines 131-148): the paren
		// flag tells closeRange to stop at the offset. A range
		// already closed inside the parentheses was built by the
		// bare-selector productions instead, and keeps its
		// post-range stages (lines 149-152 wrapped by 153).
		unit.paren = true
		r, err := p.closeRange(unit)
		if err != nil {
			return nil, nil, false, err
		}
		return r, nil, true, nil
	}
	if complete && p.cur().Kind == tokLBracket {
		return nil, nil, false, badQuery(p.cur().Pos, "unexpected range after a completed range")
	}
	return expr, unit, complete, nil
}

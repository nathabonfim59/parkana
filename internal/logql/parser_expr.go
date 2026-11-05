// The metric side of the A2 grammar: scalar literals, range and vector
// aggregations (with Loki's arity, parameter and grouping validation), and
// binary operations with PromQL precedence (syntax.y lines 89-95):
// or < and/unless < comparisons < +- < */% < ^ (right-associative).
//
// Operand typing follows Loki's mustNewBinOpExpr: log queries and bare log
// ranges cannot be binary operands, and the set operations and/or/unless
// reject literal (constant) legs — including constant subexpressions like
// (1+2), which Loki folds to a literal at parse time. Scalar/scalar,
// scalar/vector and vector/vector arithmetic and comparisons parse; the
// logical/set operations and any on/ignoring/group_* matching are deferred
// families and classify plan.ErrUnsupported after full validation.
//
// Deferred functions — label_replace(), variants(), rate_counter and
// approx_topk — parse with the same rigor as supported ones (arguments,
// types and label_replace's regex all validate) before the query defers.
// vector(NUMBER) is supported: the grammar takes a literal number only
// (syntax.y:470-472), so it builds a VectorLiteral rather than parsing an
// arbitrary scalar expression.

package logql

import (
	"regexp"
	"strconv"
	"strings"
)

// parseExpr parses a complete expression by precedence climbing.
func (p *parser) parseExpr() (Expr, error) {
	return p.parseBinaryExpr(1)
}

// binOpAt maps a token to its binary operator, if it is one. Keywords match
// case-insensitively, like Loki's token table.
func binOpAt(t token) (BinOp, bool) {
	switch t.Kind {
	case tokPlus:
		return OpAdd, true
	case tokMinus:
		return OpSub, true
	case tokStar:
		return OpMul, true
	case tokSlash:
		return OpDiv, true
	case tokPercent:
		return OpMod, true
	case tokCaret:
		return OpPow, true
	case tokEq:
		return OpEq, true
	case tokNeq:
		return OpNeq, true
	case tokGt:
		return OpGt, true
	case tokGte:
		return OpGte, true
	case tokLt:
		return OpLt, true
	case tokLte:
		return OpLte, true
	case tokIdent:
		switch strings.ToLower(t.Text) {
		case "and":
			return OpAnd, true
		case "or":
			return OpOr, true
		case "unless":
			return OpUnless, true
		}
	}
	return 0, false
}

// parseBinaryExpr climbs from minPrec (1 loosest). Unary signs only apply
// to number literals (Loki's literalExpr), so the operand parser handles
// them and every other operator is binary.
func (p *parser) parseBinaryExpr(minPrec int) (Expr, error) {
	if err := p.enter(p.cur().Pos); err != nil {
		return nil, err
	}
	defer p.leave()
	lhs, err := p.parseMetricOperand()
	if err != nil {
		return nil, err
	}
	lhsDepth := exprDepth(lhs)
	for {
		op, ok := binOpAt(p.cur())
		if !ok || op.precedence() < minPrec {
			return lhs, nil
		}
		opTok := p.next()
		e := &BinaryExpr{Pos: opTok.Pos, Op: op, LHS: lhs}
		if identIs(p.cur(), "bool") {
			p.next()
			e.Bool = true
		}
		if identIs(p.cur(), "on") || identIs(p.cur(), "ignoring") {
			e.Matching = &VectorMatching{Pos: p.cur().Pos, On: identIs(p.cur(), "on")}
			p.next()
			if e.Matching.Names, err = p.parseParenLabels("vector matching"); err != nil {
				return nil, err
			}
			if identIs(p.cur(), "group_left") || identIs(p.cur(), "group_right") {
				e.Matching.Card = matchCard(p.cur().Text)
				p.next()
				if p.cur().Kind == tokLParen {
					if e.Matching.Include, err = p.parseParenLabels("vector matching"); err != nil {
						return nil, err
					}
				}
			}
		} else if identIs(p.cur(), "group_left") || identIs(p.cur(), "group_right") {
			// group_left/group_right only follow on/ignoring in Loki's
			// binOpModifier productions.
			return nil, badQuery(p.cur().Pos, "%s requires on() or ignoring() before it", p.cur().Text)
		}
		nextMin := op.precedence() + 1
		if op.rightAssociative() {
			nextMin = op.precedence()
		}
		rhs, err := p.parseBinaryExpr(nextMin)
		if err != nil {
			return nil, err
		}
		e.RHS = rhs
		// Type checks in Loki's mustNewBinOpExpr order: operand kinds
		// first, then literal legs on set operations. Loki folds literal
		// binops to literals at parse time, so a constant subexpression
		// like (1+2) counts as a literal here.
		if !isMetricExpr(e.LHS) {
			return nil, badQuery(opTok.Pos, "unexpected type for left leg of binary operation (%s): a log query or range cannot be an operand", op)
		}
		if !isMetricExpr(e.RHS) {
			return nil, badQuery(opTok.Pos, "unexpected type for right leg of binary operation (%s): a log query or range cannot be an operand", op)
		}
		if (op == OpAnd || op == OpOr || op == OpUnless) && (isConstExpr(e.LHS) || isConstExpr(e.RHS)) {
			return nil, badQuery(opTok.Pos, "unexpected literal for logical/set binary operation (%s)", op)
		}
		if e.Matching != nil {
			p.deferUnsupported(e.Matching.Pos, "on/ignoring/group_left/group_right vector matching is not supported")
		} else if op == OpAnd || op == OpOr || op == OpUnless {
			p.deferUnsupported(opTok.Pos, "logical/set binary operations are not supported")
		}
		depth := 1 + max(lhsDepth, exprDepth(e.RHS))
		if depth > maxExprDepth {
			return nil, badQuery(opTok.Pos, "expression nested deeper than %d levels", maxExprDepth)
		}
		lhsDepth = depth
		lhs = e
	}
}

func matchCard(name string) MatchCard {
	if strings.EqualFold(name, "group_left") {
		return MatchManyToOne
	}
	return MatchOneToMany
}

// parseParenLabels parses '(' [name {',' name}] ')', the labels rule.
func (p *parser) parseParenLabels(context string) ([]string, error) {
	if _, err := p.expect(tokLParen, context); err != nil {
		return nil, err
	}
	var names []string
	if p.cur().Kind == tokRParen {
		p.next()
		return names, nil
	}
	for {
		name, err := p.expectLabelName(context)
		if err != nil {
			return nil, err
		}
		names = append(names, name.Text)
		if p.cur().Kind != tokComma {
			if _, err := p.expect(tokRParen, context); err != nil {
				return nil, err
			}
			return names, nil
		}
		p.next()
	}
}

// parseMetricOperand parses one binary-operation operand: a number literal
// (with optional sign, Loki's literalExpr), an aggregation or deferred
// function, a parenthesized expression, or — so binary type checks see them
// — a log query or log range.
func (p *parser) parseMetricOperand() (Expr, error) {
	t := p.cur()
	switch {
	case t.Kind == tokPlus || t.Kind == tokMinus:
		neg := t.Kind == tokMinus
		pos := p.next()
		num, err := p.expect(tokNumber, "scalar literal")
		if err != nil {
			return nil, err
		}
		v := num.Num
		if neg {
			v = -v
		}
		return &NumberLit{Pos: pos.Pos, V: v}, nil
	case t.Kind == tokNumber:
		p.next()
		return &NumberLit{Pos: t.Pos, V: t.Num}, nil
	case t.Kind == tokLBrace:
		return p.finishLogUnit()
	case t.Kind == tokLParen:
		if p.parenIsLog() {
			e, _, _, err := p.parseLogParen()
			return e, err
		}
		if err := p.enter(t.Pos); err != nil {
			return nil, err
		}
		p.next()
		e, err := p.parseBinaryExpr(1)
		if err != nil {
			return nil, err
		}
		p.leave()
		if _, err := p.expect(tokRParen, "parenthesized expression"); err != nil {
			return nil, err
		}
		return e, nil
	case t.Kind == tokIdent && p.looksLikeFunction(p.i):
		return p.parseFunctionCall()
	case t.Kind == tokIdent:
		return nil, badQuery(t.Pos, "unexpected identifier %s: expected a number, function, selector or '('", t)
	}
	return nil, badQuery(t.Pos, "unexpected %s: expected a number, function, selector or '('", t)
}

// finishLogUnit parses a log query starting at '{' and, when the grammar
// allows it there, its range: a unit with an unwrap or a following range
// becomes a LogRange (invalid at the root, but the caller's type checks
// report that); otherwise it stays a LogQuery.
func (p *parser) finishLogUnit() (Expr, error) {
	u, err := p.parseLogUnit()
	if err != nil {
		return nil, err
	}
	if u.uw != nil && p.cur().Kind != tokLBracket {
		return nil, badQuery(u.uw.Pos, "unwrap requires a range")
	}
	if p.cur().Kind == tokLBracket {
		return p.closeRange(u)
	}
	return u.q, nil
}

// parseFunctionCall dispatches a function keyword at the cursor (already
// known to look like a function) to its parser.
func (p *parser) parseFunctionCall() (Expr, error) {
	name := p.cur()
	switch lower := strings.ToLower(name.Text); lower {
	case "sum", "avg", "min", "max", "count", "stddev", "stdvar",
		"topk", "bottomk", "sort", "sort_desc":
		fn, _ := ParseVectorFunc(lower)
		return p.parseVectorAgg(fn)
	case "approx_topk":
		return p.parseApproxTopK()
	case "count_over_time", "rate", "bytes_over_time", "bytes_rate",
		"sum_over_time", "avg_over_time", "min_over_time", "max_over_time",
		"stddev_over_time", "stdvar_over_time", "first_over_time",
		"last_over_time", "quantile_over_time", "absent_over_time":
		fn, _ := ParseRangeFunc(lower)
		return p.parseRangeAgg(fn)
	case "rate_counter":
		return p.parseRangeAgg(-1)
	case "vector":
		return p.parseVectorFunc()
	case "label_replace":
		return p.parseLabelReplace()
	case "variants":
		return p.parseVariants()
	default:
		return nil, badQuery(name.Pos, "unknown function %s", name.Text)
	}
}

// parseVectorAgg parses a vector aggregation with optional grouping before
// and/or after the parentheses (both placements are Loki grammar, but not
// both at once) and the topk/bottomk parameter, mirroring
// mustNewVectorAggregationExpr's checks.
func (p *parser) parseVectorAgg(fn VectorFunc) (Expr, error) {
	fnTok := p.next()
	var grouping *Grouping
	var err error
	if atGrouping(p.cur()) {
		if grouping, err = p.parseGrouping(); err != nil {
			return nil, err
		}
	}
	if _, err := p.expect(tokLParen, fn.String()); err != nil {
		return nil, err
	}
	a := &VectorAgg{Pos: fnTok.Pos, Func: fn, Grouping: grouping}
	if p.cur().Kind == tokNumber && p.peek(1).Kind == tokComma {
		param := p.next()
		p.next() // ','
		if fn != TopK && fn != BottomK {
			return nil, badQuery(param.Pos, "unsupported parameter for operation %s(%s", fn, param.Text)
		}
		k, conv := strconv.Atoi(param.Text)
		if conv != nil {
			return nil, badQuery(param.Pos, "invalid parameter %s(%s,", fn, param.Text)
		}
		if k <= 0 {
			return nil, badQuery(param.Pos, "invalid parameter (must be greater than 0) %s(%s", fn, param.Text)
		}
		a.Params = []float64{float64(k)}
	} else if fn == TopK || fn == BottomK {
		return nil, badQuery(fnTok.Pos, "parameter required for operation %s", fn)
	}
	if a.Left, err = p.parseBinaryExpr(1); err != nil {
		return nil, err
	}
	if !isMetricExpr(a.Left) {
		return nil, badQuery(fnTok.Pos, "aggregation argument of %s must be a metric expression", fn)
	}
	if _, err := p.expect(tokRParen, fn.String()); err != nil {
		return nil, err
	}
	if atGrouping(p.cur()) {
		if grouping != nil {
			return nil, badQuery(p.cur().Pos, "grouping specified twice in %s aggregation", fn)
		}
		if a.Grouping, err = p.parseGrouping(); err != nil {
			return nil, err
		}
	}
	// sort and sort_desc order by value, not labels: Loki's
	// validateSortGrouping (pinned parser.go lines 253-258) rejects any
	// non-empty by/without on them, keeping the empty forms valid.
	if (fn == Sort || fn == SortDesc) && a.Grouping != nil && len(a.Grouping.Names) > 0 {
		return nil, badQuery(a.Grouping.Pos, "sort and sort_desc don't allow grouping")
	}
	return a, nil
}

// parseApproxTopK parses approx_topk with Loki's checks (parameter
// required, grouping not allowed) and defers the family.
func (p *parser) parseApproxTopK() (Expr, error) {
	fnTok := p.next()
	var err error
	if _, err := p.expect(tokLParen, "approx_topk"); err != nil {
		return nil, err
	}
	if p.cur().Kind != tokNumber || p.peek(1).Kind != tokComma {
		return nil, badQuery(fnTok.Pos, "parameter required for operation approx_topk")
	}
	param := p.next()
	p.next() // ','
	k, conv := strconv.Atoi(param.Text)
	if conv != nil {
		return nil, badQuery(param.Pos, "invalid parameter approx_topk(%s,", param.Text)
	}
	if k <= 0 {
		return nil, badQuery(param.Pos, "invalid parameter (must be greater than 0) approx_topk(%s", param.Text)
	}
	arg, err := p.parseBinaryExpr(1)
	if err != nil {
		return nil, err
	}
	if !isMetricExpr(arg) {
		return nil, badQuery(fnTok.Pos, "aggregation argument of approx_topk must be a metric expression")
	}
	if _, err := p.expect(tokRParen, "approx_topk"); err != nil {
		return nil, err
	}
	if atGrouping(p.cur()) {
		if _, err := p.parseGrouping(); err != nil {
			return nil, err
		}
		return nil, badQuery(p.cur().Pos, "grouping not allowed for approx_topk aggregation")
	}
	p.deferUnsupported(fnTok.Pos, "approx_topk is not supported")
	return &deferredExpr{pos: fnTok.Pos, what: "approx_topk(" + param.Text + ", …)"}, nil
}

// atGrouping reports whether the cursor is at a by/without grouping.
func atGrouping(t token) bool {
	return t.Kind == tokIdent && (strings.EqualFold(t.Text, "by") || strings.EqualFold(t.Text, "without"))
}

// parseGrouping parses by (…) or without (…), including the empty form.
func (p *parser) parseGrouping() (*Grouping, error) {
	g := &Grouping{Pos: p.cur().Pos, Without: strings.EqualFold(p.cur().Text, "without")}
	p.next()
	names, err := p.parseParenLabels("grouping")
	if err != nil {
		return nil, err
	}
	g.Names = names
	return g, nil
}

// parseRangeAgg parses a range aggregation. fn < 0 means the deferred
// rate_counter, which follows the same productions. Parameter, grouping and
// unwrap rules mirror newRangeAggregationExpr and its validate().
func (p *parser) parseRangeAgg(fn RangeFunc) (Expr, error) {
	fnTok := p.next()
	fnName := fn.String()
	if fn < 0 {
		fnName = "rate_counter"
	}
	var params []float64
	var err error
	if _, err := p.expect(tokLParen, fnName); err != nil {
		return nil, err
	}
	if p.cur().Kind == tokNumber && p.peek(1).Kind == tokComma {
		param := p.next()
		p.next() // ','
		if fn != QuantileOverTime {
			return nil, badQuery(param.Pos, "parameter %s not supported for operation %s", param.Text, fnName)
		}
		params = []float64{param.Num}
	} else if fn == QuantileOverTime {
		return nil, badQuery(fnTok.Pos, "parameter required for operation %s", fnName)
	}
	r, err := p.parseLogRangeArg(fnName)
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(tokRParen, fnName); err != nil {
		return nil, err
	}
	a := &RangeAgg{Pos: fnTok.Pos, Func: fn, Params: params, Range: r}
	if atGrouping(p.cur()) {
		if a.Grouping, err = p.parseGrouping(); err != nil {
			return nil, err
		}
		if !groupingAllowedRange(fn) {
			return nil, badQuery(a.Grouping.Pos, "grouping not allowed for %s aggregation", fnName)
		}
	}
	if r.Unwrap != nil && !unwrapAllowedRange(fn) {
		return nil, badQuery(r.Unwrap.Pos, "invalid aggregation %s with unwrap", fnName)
	}
	if r.Unwrap == nil && !plainAllowedRange(fn) {
		return nil, badQuery(fnTok.Pos, "invalid aggregation %s without unwrap", fnName)
	}
	if fn < 0 {
		p.deferUnsupported(fnTok.Pos, "rate_counter is not supported")
		return &deferredExpr{pos: fnTok.Pos, what: fnName + "(…)"}, nil
	}
	return a, nil
}

// groupingAllowedRange mirrors Loki's RangeAggregationExpr.validate grouping
// list: only these take by/without.
func groupingAllowedRange(fn RangeFunc) bool {
	switch fn {
	case AvgOverTime, StddevOverTime, StdvarOverTime, QuantileOverTime,
		MaxOverTime, MinOverTime, FirstOverTime, LastOverTime:
		return true
	}
	return false
}

// unwrapAllowedRange mirrors validate's unwrap list. fn < 0 is the deferred
// rate_counter, which Loki allows with unwrap.
func unwrapAllowedRange(fn RangeFunc) bool {
	if fn < 0 {
		return true
	}
	switch fn {
	case AvgOverTime, SumOverTime, MaxOverTime, MinOverTime, StddevOverTime,
		StdvarOverTime, QuantileOverTime, Rate, AbsentOverTime,
		FirstOverTime, LastOverTime:
		return true
	}
	return false
}

// plainAllowedRange mirrors validate's without-unwrap list.
func plainAllowedRange(fn RangeFunc) bool {
	switch fn {
	case BytesOverTime, BytesRate, CountOverTime, Rate, AbsentOverTime:
		return true
	}
	return false
}

// parseLogRangeArg parses the logRangeExpr argument of a range aggregation:
// a bare selector-based range or a parenthesis layer around one.
func (p *parser) parseLogRangeArg(fnName string) (*LogRange, error) {
	switch p.cur().Kind {
	case tokLBrace:
		u, err := p.parseLogUnit()
		if err != nil {
			return nil, err
		}
		if p.cur().Kind != tokLBracket {
			return nil, badQuery(p.cur().Pos, "expected range: %s needs a [duration] selector", fnName)
		}
		return p.closeRange(u)
	case tokLParen:
		if !p.parenIsLog() {
			return nil, badQuery(p.cur().Pos, "expected selector in %s argument", fnName)
		}
		e, _, _, err := p.parseLogParen()
		if err != nil {
			return nil, err
		}
		switch r := e.(type) {
		case *LogRange:
			return r, nil
		case *LogQuery:
			return nil, badQuery(p.cur().Pos, "expected range: %s needs a [duration] selector", fnName)
		}
	}
	return nil, badQuery(p.cur().Pos, "expected selector in %s argument, found %s", fnName, p.cur())
}

// parseVectorFunc parses vector(number). Loki's production takes a bare
// NUMBER (syntax.y:470-472), not a signed literal or scalar expression, so
// vector(-1) and vector(1+1) stay bad queries.
func (p *parser) parseVectorFunc() (Expr, error) {
	fnTok := p.next()
	if _, err := p.expect(tokLParen, "vector"); err != nil {
		return nil, err
	}
	num, err := p.expect(tokNumber, "vector")
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(tokRParen, "vector"); err != nil {
		return nil, err
	}
	return &VectorLiteral{Pos: fnTok.Pos, V: num.Num}, nil
}

// parseLabelReplace parses label_replace(expr, dst, replacement, src, regex)
// with its regex validated, as Loki's mustNewLabelReplaceExpr does, and
// defers the family.
func (p *parser) parseLabelReplace() (Expr, error) {
	fnTok := p.next()
	if _, err := p.expect(tokLParen, "label_replace"); err != nil {
		return nil, err
	}
	arg, err := p.parseBinaryExpr(1)
	if err != nil {
		return nil, err
	}
	if !isMetricExpr(arg) {
		return nil, badQuery(fnTok.Pos, "first argument of label_replace must be a metric expression")
	}
	args := make([]string, 4)
	for i := range args {
		if _, err := p.expect(tokComma, "label_replace"); err != nil {
			return nil, err
		}
		s, err := p.expect(tokString, "label_replace")
		if err != nil {
			return nil, err
		}
		args[i] = s.Str
	}
	if _, err := p.expect(tokRParen, "label_replace"); err != nil {
		return nil, err
	}
	if _, err := regexp.Compile("^(?:" + args[3] + ")$"); err != nil {
		return nil, badQuery(fnTok.Pos, "invalid regex in label_replace: %v", err)
	}
	p.deferUnsupported(fnTok.Pos, "label_replace is not supported")
	return &deferredExpr{pos: fnTok.Pos, what: "label_replace(…)"}, nil
}

// parseVariants parses variants(metric, …) of (logRange) and defers it.
func (p *parser) parseVariants() (Expr, error) {
	fnTok := p.next()
	if _, err := p.expect(tokLParen, "variants"); err != nil {
		return nil, err
	}
	for {
		arg, err := p.parseBinaryExpr(1)
		if err != nil {
			return nil, err
		}
		if !isMetricExpr(arg) {
			return nil, badQuery(fnTok.Pos, "variants takes metric expressions")
		}
		if p.cur().Kind != tokComma {
			break
		}
		p.next()
	}
	if _, err := p.expect(tokRParen, "variants"); err != nil {
		return nil, err
	}
	of := p.cur()
	if !identIs(of, "of") {
		return nil, badQuery(of.Pos, "expected 'of' in variants, found %s", of)
	}
	p.next()
	if _, err := p.expect(tokLParen, "variants"); err != nil {
		return nil, err
	}
	if _, err := p.parseLogRangeArg("variants"); err != nil {
		return nil, err
	}
	if _, err := p.expect(tokRParen, "variants"); err != nil {
		return nil, err
	}
	p.deferUnsupported(fnTok.Pos, "variants is not supported")
	return &deferredExpr{pos: fnTok.Pos, what: "variants(…) of (…)"}, nil
}

// isMetricExpr reports whether e can be a metric expression (Loki's
// SampleExpr): literals, vector literals, aggregations, binary expressions
// and the deferred functions. Log queries and bare log ranges cannot.
func isMetricExpr(e Expr) bool {
	switch e.(type) {
	case *NumberLit, *VectorLiteral, *RangeAgg, *VectorAgg, *BinaryExpr, *deferredExpr:
		return true
	}
	return false
}

// isConstExpr reports whether e is a constant scalar: a literal, or a
// binary expression over constants, which Loki folds to a literal at parse
// time (reduceBinOp) and then rejects as a logical/set operation leg.
func isConstExpr(e Expr) bool {
	switch e := e.(type) {
	case *NumberLit:
		return true
	case *BinaryExpr:
		return isConstExpr(e.LHS) && isConstExpr(e.RHS)
	}
	return false
}

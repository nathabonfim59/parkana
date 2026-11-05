// Scalar and binary expression evaluation: scalar arithmetic and comparisons,
// scalar/vector operations in both operand orders, default vector/vector
// arithmetic and comparisons, and the vector(NUMBER) literal.
//
// The result type is a property of the expression tree, never of a runtime
// sample set: a comparison over an empty vector is still a vector, and a
// comparison over two literals is still a scalar. exprKind makes that
// distinction; Loki's evaluator does the same with its SampleExpr type switch
// (pkg/logql/evaluator.go NewStepEvaluator).
//
// The arithmetic is Loki 3.7.9's MergeBinOp (pkg/logql/syntax/ast.go
// 1938-2146): +, -, * and ^ are the Go operations, / and % return NaN when the
// divisor is zero (not ±Inf), and comparisons produce 1/0. A vector-side
// operand keeps its labels: with no matching modifier, resultMetric deletes no
// labels, so __name__ and every other label survive. A non-bool comparison
// filters (only true matches survive) and keeps the vector-side sample's value,
// while `bool` emits 0/1 for every matched sample (MergeBinOp's filter/swap
// arguments, as used by LiteralStepEvaluator and vectorBinop).
//
// Vector/vector operations use one-to-one matching on the complete label set:
// matchingSignature hashes the whole metric when there is no on/ignoring
// modifier (evaluator.go), so only samples with identical labels pair up and
// unmatched samples disappear. Duplicate signatures are client errors with
// Loki's messages. Explicit on/ignoring/group_left/group_right and the
// and/or/unless set operations stay plan.ErrUnsupported; the parser validates
// them completely before deferring.
//
// vector(NUMBER) is one empty-label sample at every evaluation step. Loki's
// grammar takes a bare NUMBER (syntax.y:470-472) and its evaluator emits a
// sample with labels.EmptyLabels for each step from Start to End inclusive
// (evaluator.go VectorIterator); an instant query is the single Start sample
// (pkg/logql/engine.go evalVector, 712-731). Scalar roots are Loki's
// evalLiteral (engine.go 695-710): a scalar result for an instant query and a
// single label-less matrix series with one point per step for a range query
// (PopulateMatrixFromScalar, engine.go 733-755).
package logql

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/nathabonfim59/parkana/internal/plan"
)

// metricExprKind is a metric expression's result type. It is syntactic:
// NumberLit is a scalar, VectorLiteral and the aggregations are vectors, and
// a BinaryExpr is a vector as soon as either leg is one.
type metricExprKind int

const (
	scalarExpr metricExprKind = iota
	vectorExpr
)

// exprKind classifies a metric expression without evaluating it.
func exprKind(e Expr) (metricExprKind, error) {
	switch x := e.(type) {
	case *NumberLit:
		return scalarExpr, nil
	case *VectorLiteral, *RangeAgg, *VectorAgg:
		return vectorExpr, nil
	case *BinaryExpr:
		if x == nil || x.LHS == nil || x.RHS == nil {
			return 0, errors.New("logql: binary expression without operands")
		}
		left, err := exprKind(x.LHS)
		if err != nil {
			return 0, err
		}
		right, err := exprKind(x.RHS)
		if err != nil {
			return 0, err
		}
		if left == vectorExpr || right == vectorExpr {
			return vectorExpr, nil
		}
		return scalarExpr, nil
	case *deferredExpr:
		if x == nil {
			return 0, errors.New("logql: nil deferred expression")
		}
		return 0, fmt.Errorf("logql: %w: %s is not supported", plan.ErrUnsupported, x.what)
	case nil:
		return 0, errors.New("logql: nil metric expression")
	default:
		return 0, fmt.Errorf("logql: %w: unexpected metric expression %T", plan.ErrUnsupported, e)
	}
}

// evalScalar evaluates a scalar expression at the request's times. An instant
// query is a scalar result; a range query is one label-less matrix series with
// a point at every step, matching Loki's PopulateMatrixFromScalar. The direct
// root timestamp is UnixNano()/1e6 (truncation), not UnixMilli.
func (e *Engine) evalScalar(ctx context.Context, expr Expr, p plan.Params) (plan.Result, error) {
	if err := ctx.Err(); err != nil {
		return plan.Result{}, fmt.Errorf("logql: scalar expression: %w", err)
	}
	value, err := scalarValue(expr)
	if err != nil {
		return plan.Result{}, err
	}
	if p.Instant() {
		return plan.Result{Type: plan.TypeScalar, Scalar: plan.Point{T: truncMilli(p.Start), V: value}}, nil
	}
	times, err := metricEvalTimes(p)
	if err != nil {
		return plan.Result{}, err
	}
	if err := ctx.Err(); err != nil {
		return plan.Result{}, fmt.Errorf("logql: scalar expression: %w", err)
	}
	grid := nominalGrid(times)
	points := make([]plan.Point, len(times))
	for i := range times {
		points[i] = plan.Point{T: grid.outs[i], V: value}
	}
	return plan.Result{Type: plan.TypeMatrix, Matrix: []plan.Series{{Points: points}}}, nil
}

// evalVectorLiteral evaluates a direct root vector(NUMBER). An instant query
// is one empty-label sample at Start.UnixMilli (Loki's evalVector); a range
// query is one series on the exact request grid with UnixNano()/1e6 timestamps
// (PopulateMatrixFromScalar). A vector literal nested in another expression
// uses the VectorIterator grid instead; see evalVectorSeriesExpr.
func (e *Engine) evalVectorLiteral(ctx context.Context, v *VectorLiteral, p plan.Params) (plan.Result, error) {
	if err := ctx.Err(); err != nil {
		return plan.Result{}, fmt.Errorf("logql: vector literal: %w", err)
	}
	if v == nil {
		return plan.Result{}, errors.New("logql: nil vector literal")
	}
	var grid *evalGrid
	if p.Instant() {
		at := time.UnixMilli(p.Start.UnixMilli()).UTC()
		grid = &evalGrid{times: []time.Time{p.Start}, outs: []time.Time{at}}
	} else {
		times, err := metricEvalTimes(p)
		if err != nil {
			return plan.Result{}, err
		}
		grid = nominalGrid(times)
	}
	if err := ctx.Err(); err != nil {
		return plan.Result{}, fmt.Errorf("logql: vector literal: %w", err)
	}
	return e.finishMetric(vectorLiteralSeries(grid, v.V), p)
}

// evalBinary evaluates a top-level binary expression. Scalar expressions take
// the scalar shape; vector expressions assemble every step and are limited and
// shaped exactly once, over the final response.
func (e *Engine) evalBinary(ctx context.Context, src plan.Source, b *BinaryExpr, p plan.Params) (plan.Result, error) {
	if err := ctx.Err(); err != nil {
		return plan.Result{}, fmt.Errorf("logql: binary operation: %w", err)
	}
	if b == nil {
		return plan.Result{}, errors.New("logql: nil binary expression")
	}
	kind, err := exprKind(b)
	if err != nil {
		return plan.Result{}, err
	}
	if kind == scalarExpr {
		return e.evalScalar(ctx, b, p)
	}
	es, err := e.evalBinarySeriesGrid(ctx, src, b, p)
	if err != nil {
		return plan.Result{}, err
	}
	if err := ctx.Err(); err != nil {
		return plan.Result{}, fmt.Errorf("logql: binary operation: %w", err)
	}
	return e.finishBinaryMetric(es, p, exprHasSort(b))
}

// evalBinarySeriesGrid is the recursive seam for vector-typed binary
// expressions. It returns every output series with its grid and applies no
// series limit: an enclosing expression or the top-level response owns
// MaxSeries. Scalar operands are evaluated once to a value; vector operands
// recurse through evalVectorSeriesExpr.
//
// Two vector legs synchronize by evaluation index, not by timestamp: Loki's
// BinOpStepEvaluator advances both evaluators and returns the left leg's
// timestamp, so the output grid is the left grid truncated to the shorter
// child (or the vector side's grid when the left leg is a scalar). That keeps
// a nested vector literal's different step count from inventing or dropping
// output samples beyond the common prefix.
func (e *Engine) evalBinarySeriesGrid(ctx context.Context, src plan.Source, b *BinaryExpr, p plan.Params) (*evalSeries, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("logql: binary operation: %w", err)
	}
	if b == nil || b.LHS == nil || b.RHS == nil {
		return nil, errors.New("logql: binary expression without operands")
	}
	kind, err := exprKind(b)
	if err != nil {
		return nil, err
	}
	if kind != vectorExpr {
		return nil, fmt.Errorf("logql: %w: aggregation over a scalar expression is not supported", plan.ErrUnsupported)
	}
	leftKind, err := exprKind(b.LHS)
	if err != nil {
		return nil, err
	}
	rightKind, err := exprKind(b.RHS)
	if err != nil {
		return nil, err
	}

	var (
		left, right             *evalSeries
		leftScalar, rightScalar float64
		leftInputs, rightInputs []vectorInput
	)
	if leftKind == scalarExpr {
		if leftScalar, err = scalarValue(b.LHS); err != nil {
			return nil, err
		}
	} else {
		if left, err = e.evalVectorSeriesExpr(ctx, src, b.LHS, p); err != nil {
			return nil, err
		}
	}
	// A Source call or a recursive child may have taken a while; stop before
	// the CPU-bound step loop if the request was canceled meanwhile.
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("logql: binary operation: %w", err)
	}
	if rightKind == scalarExpr {
		if rightScalar, err = scalarValue(b.RHS); err != nil {
			return nil, err
		}
	} else {
		if right, err = e.evalVectorSeriesExpr(ctx, src, b.RHS, p); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("logql: binary operation: %w", err)
	}

	var grid *evalGrid
	switch {
	case left == nil && right == nil:
		// Unreachable: a scalar/scalar expression is evaluated by evalScalar
		// and never becomes a vector child.
		return nil, errors.New("logql: scalar binary expression in vector context")
	case left == nil:
		grid = right.grid
	case right == nil:
		grid = left.grid
	default:
		grid = left.grid.head(min(left.grid.count(), right.grid.count()))
	}
	if left != nil {
		if leftInputs, err = indexVectorPoints(left.grid.times, left.series); err != nil {
			return nil, err
		}
	}
	if right != nil {
		if rightInputs, err = indexVectorPoints(right.grid.times, right.series); err != nil {
			return nil, err
		}
	}

	set := &metricSet{}
	for step, at := range grid.times {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("logql: binary operation: %w", err)
		}
		switch {
		case left == nil:
			if err := binaryScalarStep(ctx, b, step, at, rightInputs, leftScalar, true, set); err != nil {
				return nil, err
			}
		case right == nil:
			if err := binaryScalarStep(ctx, b, step, at, leftInputs, rightScalar, false, set); err != nil {
				return nil, err
			}
		default:
			if err := binaryVectorStep(ctx, b, step, at, leftInputs, rightInputs, set); err != nil {
				return nil, err
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("logql: binary operation: %w", err)
	}
	if err := tagSeries(set.series, grid); err != nil {
		return nil, err
	}
	return &evalSeries{grid: grid, series: set.series}, nil
}

// evalVectorSeriesExpr evaluates any vector-typed expression into unrestricted
// series and the grid their steps belong to. It is the single recursive entry
// point for binary operands and vector aggregation children.
func (e *Engine) evalVectorSeriesExpr(ctx context.Context, src plan.Source, expr Expr, p plan.Params) (*evalSeries, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("logql: vector expression: %w", err)
	}
	switch x := expr.(type) {
	case *RangeAgg:
		times, err := metricEvalTimes(p)
		if err != nil {
			return nil, err
		}
		series, err := e.evalRangeSeries(ctx, src, x, p, rangeOptions{aggregate: true, child: true})
		if err != nil {
			return nil, err
		}
		return &evalSeries{grid: rangeGrid(times, x.Range.Offset), series: series}, nil
	case *VectorAgg:
		return e.evalVectorSeriesGrid(ctx, src, x, p)
	case *VectorLiteral:
		if x == nil {
			return nil, errors.New("logql: nil vector literal")
		}
		grid, err := vectorIterGrid(p)
		if err != nil {
			return nil, err
		}
		return &evalSeries{grid: grid, series: vectorLiteralSeries(grid, x.V)}, nil
	case *BinaryExpr:
		return e.evalBinarySeriesGrid(ctx, src, x, p)
	case *deferredExpr:
		if x == nil {
			return nil, errors.New("logql: nil deferred expression")
		}
		return nil, fmt.Errorf("logql: %w: %s is not supported", plan.ErrUnsupported, x.what)
	case nil:
		return nil, errors.New("logql: nil vector expression")
	default:
		return nil, fmt.Errorf("logql: %w: unexpected vector expression %T", plan.ErrUnsupported, expr)
	}
}

// binaryScalarStep applies one step of a scalar/vector operation. scalarLeft
// selects the operand order, which matters for -, /, %, ^ and comparisons.
// The result keeps the vector sample's labels; a non-bool comparison filters
// and retains the vector-side value, while bool emits 0/1 for every present
// sample (Loki's LiteralStepEvaluator + MergeBinOp).
func binaryScalarStep(ctx context.Context, b *BinaryExpr, step int, at time.Time, vec []vectorInput, scalar float64, scalarLeft bool, set *metricSet) error {
	for i := range vec {
		// One step can carry a large intermediate vector; recheck the
		// request periodically rather than only between steps.
		if i&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return fmt.Errorf("logql: binary operation: %w", err)
			}
		}
		value, ok := vec[i].values[step]
		if !ok {
			continue
		}
		left, right := scalar, value
		if !scalarLeft {
			left, right = value, scalar
		}
		result, truth, err := binOpValue(b.Op, left, right)
		if err != nil {
			return err
		}
		emit := !b.Op.IsComparison() || b.Bool
		if !emit && !truth {
			continue
		}
		out := result
		if !emit {
			// The filtering comparison returns the vector-side sample, not
			// the comparison's 1: MergeBinOp's retSample.
			out = value
		}
		ms := set.get(vec[i].labels)
		ms.points = append(ms.points, plan.Point{T: at, V: out})
	}
	return nil
}

// binaryVectorStep applies one step of a vector/vector operation. Matching is
// one-to-one over the complete label set (matchingSignature with no
// on/ignoring modifier); unmatched samples disappear. The result keeps the
// left-hand labels, which is resultMetric's behavior when no matching labels
// are removed.
func binaryVectorStep(ctx context.Context, b *BinaryExpr, step int, at time.Time, lhs, rhs []vectorInput, set *metricSet) error {
	right := make(map[string]float64, len(rhs))
	for i := range rhs {
		if i&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return fmt.Errorf("logql: binary operation: %w", err)
			}
		}
		value, ok := rhs[i].values[step]
		if !ok {
			continue
		}
		key := rhs[i].labels.String()
		if _, duplicate := right[key]; duplicate {
			return fmt.Errorf("logql: %w: found duplicate series on the right hand-side; many-to-many matching not allowed: matching labels must be unique on one side", plan.ErrBadQuery)
		}
		right[key] = value
	}
	matched := make(map[string]struct{}, len(lhs))
	for i := range lhs {
		if i&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return fmt.Errorf("logql: binary operation: %w", err)
			}
		}
		value, ok := lhs[i].values[step]
		if !ok {
			continue
		}
		key := lhs[i].labels.String()
		other, found := right[key]
		if !found {
			continue
		}
		if _, duplicate := matched[key]; duplicate {
			return fmt.Errorf("logql: %w: multiple matches for labels: many-to-one matching must be explicit (group_left/group_right)", plan.ErrBadQuery)
		}
		matched[key] = struct{}{}
		result, truth, err := binOpValue(b.Op, value, other)
		if err != nil {
			return err
		}
		switch {
		case !b.Op.IsComparison(), b.Bool:
			ms := set.get(lhs[i].labels)
			ms.points = append(ms.points, plan.Point{T: at, V: result})
		case truth:
			ms := set.get(lhs[i].labels)
			ms.points = append(ms.points, plan.Point{T: at, V: value})
		}
	}
	return nil
}

// scalarValue evaluates a scalar expression tree. Only literals and binary
// expressions over them are scalars; anything else is a caller bug.
func scalarValue(e Expr) (float64, error) {
	switch x := e.(type) {
	case *NumberLit:
		if x == nil {
			return 0, errors.New("logql: nil scalar literal")
		}
		return x.V, nil
	case *BinaryExpr:
		if x == nil || x.LHS == nil || x.RHS == nil {
			return 0, errors.New("logql: binary expression without operands")
		}
		kind, err := exprKind(x)
		if err != nil {
			return 0, err
		}
		if kind != scalarExpr {
			return 0, errors.New("logql: scalar evaluation of a vector expression")
		}
		left, err := scalarValue(x.LHS)
		if err != nil {
			return 0, err
		}
		right, err := scalarValue(x.RHS)
		if err != nil {
			return 0, err
		}
		value, _, err := binOpValue(x.Op, left, right)
		return value, err
	case nil:
		return 0, errors.New("logql: nil scalar expression")
	default:
		return 0, fmt.Errorf("logql: %w: expected a scalar expression, got %T", plan.ErrUnsupported, e)
	}
}

// binOpValue applies one binary operator to two scalar values and reports the
// comparison's truth. Arithmetic always has a truth of true. Loki's MergeBinOp
// maps division and modulo by zero to NaN and leaves every other non-finite
// result to IEEE arithmetic (pkg/logql/syntax/ast.go 1978-2017).
func binOpValue(op BinOp, left, right float64) (float64, bool, error) {
	switch op {
	case OpAdd:
		return left + right, true, nil
	case OpSub:
		return left - right, true, nil
	case OpMul:
		return left * right, true, nil
	case OpDiv:
		if right == 0 {
			return math.NaN(), true, nil
		}
		return left / right, true, nil
	case OpMod:
		if right == 0 {
			return math.NaN(), true, nil
		}
		return math.Mod(left, right), true, nil
	case OpPow:
		return math.Pow(left, right), true, nil
	case OpEq:
		return comparisonValue(left == right), left == right, nil
	case OpNeq:
		return comparisonValue(left != right), left != right, nil
	case OpGt:
		return comparisonValue(left > right), left > right, nil
	case OpGte:
		return comparisonValue(left >= right), left >= right, nil
	case OpLt:
		return comparisonValue(left < right), left < right, nil
	case OpLte:
		return comparisonValue(left <= right), left <= right, nil
	}
	return 0, false, fmt.Errorf("logql: %w: binary operator %s is not supported", plan.ErrUnsupported, op)
}

// comparisonValue renders a comparison's truth as Loki's 0/1.
func comparisonValue(truth bool) float64 {
	if truth {
		return 1
	}
	return 0
}

// vectorLiteralSeries builds vector(NUMBER)'s one empty-label series on grid.
func vectorLiteralSeries(grid *evalGrid, value float64) []*metricSeries {
	if grid.count() == 0 {
		return nil
	}
	s := &metricSeries{
		points: make([]plan.Point, grid.count()),
		steps:  make([]int, grid.count()),
		outs:   make([]time.Time, grid.count()),
	}
	for i := range grid.count() {
		s.points[i] = plan.Point{T: grid.times[i], V: value}
		s.steps[i] = i
		s.outs[i] = grid.outs[i]
	}
	return []*metricSeries{s}
}

// finishBinaryMetric applies the final series limit and shapes a binary
// result. An instant response that contains sort or sort_desc keeps the
// evaluator's value order: Loki's Sortable skips the usual label sort for the
// whole expression, and JoinSampleVector returns the step vector as built
// (pkg/logql/engine.go 326-346 and 551-571).
func (e *Engine) finishBinaryMetric(es *evalSeries, p plan.Params, sortable bool) (plan.Result, error) {
	series := slices.DeleteFunc(es.series, func(s *metricSeries) bool { return len(s.points) == 0 })
	if e.cfg.MaxSeries > 0 && len(series) > e.cfg.MaxSeries {
		return plan.Result{}, fmt.Errorf("logql: %w: query returned %d series, exceeding the limit of %d; reduce cardinality with a by() aggregation", plan.ErrLimit, len(series), e.cfg.MaxSeries)
	}
	if !sortable || !p.Instant() {
		return metricResult(series, p.Instant()), nil
	}
	vec := make([]plan.Sample, 0, len(series))
	for _, s := range series {
		vec = append(vec, plan.Sample{Metric: s.labels, Point: metricPoint(s, 0)})
	}
	return plan.Result{Type: plan.TypeVector, Vector: vec}, nil
}

// exprHasSort reports whether e contains a sort or sort_desc aggregation,
// Loki's Sortable walk.
func exprHasSort(e Expr) bool {
	switch x := e.(type) {
	case *VectorAgg:
		if x == nil {
			return false
		}
		if x.Func == Sort || x.Func == SortDesc {
			return true
		}
		return exprHasSort(x.Left)
	case *BinaryExpr:
		if x == nil {
			return false
		}
		return exprHasSort(x.LHS) || exprHasSort(x.RHS)
	}
	return false
}

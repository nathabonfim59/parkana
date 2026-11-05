// Unwrap sample extraction: the | unwrap stage turns a metric pipeline into
// per-entry numeric samples. Loki compiles it to a label sample extractor
// (pkg/logql/log/metrics_extraction.go LabelExtractorWithStages) whose
// pre-stage is the ordinary pipeline, whose conversion reads one label and
// whose post stage is the label filters the grammar allows after unwrap
// (syntax.y unwrapExpr).
//
// The order is exact and observable: pipeline stages run first, then a missing
// (or empty) unwrap label drops the row without an error, then the conversion
// runs (a failure records SampleExtractionErr and replaces any earlier parser
// error, as LabelsBuilder.SetErr does), then the post filters run in order and
// may drop the row, clearing its error state with it, and only then does a
// surviving error state fail the metric query. Grouping follows
// LabelsBuilder.GroupedLabels with the grouping LabelExtractorWithStages
// derives: nil grouping is the implicit without(unwrap label), without(names)
// removes names and the unwrap label, by(names) keeps exactly the requested
// names, and explicit by () is Grouping.Singleton(): noLabels wins and yields
// an empty identity before the group lists are consulted.
package logql

import (
	"strconv"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
)

// sampleExtractionErr is Loki's errSampleExtraction (pkg/logql/log/error.go):
// the conversion of an unwrapped field failed.
const sampleExtractionErr = "SampleExtractionErr"

// convertUnwrapValue applies an unwrap conversion to the field's string value.
// Loki maps both duration and duration_seconds to time.ParseDuration and
// reports seconds; the syntactic distinction changes nothing at evaluation.
func convertUnwrapValue(fn UnwrapFunc, raw string) (float64, error) {
	switch fn {
	case UnwrapDuration, UnwrapDurationSeconds:
		return convertDuration(raw)
	case UnwrapBytes:
		return convertBytes(raw)
	default:
		return strconv.ParseFloat(raw, 64)
	}
}

// unwrapRow extracts one entry's unwrapped sample. A nil Unwrap is a plain
// line extractor (only absent_over_time can be unwrapped-free here) and yields
// the count extractor's 1. The bool is false when the row contributes no
// sample: a missing or empty unwrap label, or a post filter that rejects it.
//
// e is mutated exactly as Loki's builder is: a conversion failure sets the
// error state unconditionally (overwriting an earlier parser error) before the
// post filters run, so the filters observe and can clear it.
func unwrapRow(e *logEntry, u *Unwrap, post labelFilterer) (float64, bool) {
	if u == nil {
		return 1, true
	}
	raw, _, ok := e.get(u.Label)
	if !ok || raw == "" {
		// Not an error: Loki treats a missing unwrapped field as a row that
		// produces no sample (issue grafana/loki#6713).
		return 0, false
	}
	v, err := convertUnwrapValue(u.Func, raw)
	if err != nil {
		e.setErr(sampleExtractionErr)
		e.setErrorDetails(err.Error())
	}
	if post != nil && !post.filter(e) {
		return 0, false
	}
	return v, true
}

// compileUnwrapPostFilters compiles the filters allowed after | unwrap into
// one labelFilterer, in order, like Loki's ReduceAndLabelFilter: every filter
// is ANDed left to right and every side is evaluated, so a later filter's
// conversion error stays observable. No filters yields nil, the no-op path.
func compileUnwrapPostFilters(filters []LabelFilterExpr) (labelFilterer, error) {
	if len(filters) == 0 {
		return nil, nil
	}
	result, err := compileLabelFilterExpr(filters[0])
	if err != nil {
		return nil, err
	}
	for _, f := range filters[1:] {
		next, err := compileLabelFilterExpr(f)
		if err != nil {
			return nil, err
		}
		result = binaryLabelFilter{and: true, lhs: result, rhs: next}
	}
	return result, nil
}

// absentRangeLabels is Loki's absentLabels: the equality matchers of the range
// expression's selector, excluding __name__, with every non-equality matcher's
// label removed. Values are not unquoted or transformed; a matcher on a label
// that is also set by an earlier equality matcher deletes it, like the
// original loop.
func absentRangeLabels(matchers []plan.Matcher) logs.Labels {
	m := make(map[string]string, len(matchers))
	var drop []string
	for _, ma := range matchers {
		if ma.Name == "__name__" {
			continue
		}
		if ma.Type == plan.MatchEqual {
			if _, ok := m[ma.Name]; !ok {
				m[ma.Name] = ma.Value
				continue
			}
		}
		drop = append(drop, ma.Name)
	}
	for _, name := range drop {
		delete(m, name)
	}
	return logs.FromMap(m)
}

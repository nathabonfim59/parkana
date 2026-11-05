// Query response encoding for the Loki read API (PLAN.md §8, §11).
//
// The encoders mirror Loki 3.7.9's JSON shapes: the query envelope, streams,
// matrix, vector and scalar results, structured metadata, and the stats object
// Grafana reads. Sources for the shapes:
//
//   - github.com/grafana/loki v3.7.9 pkg/util/marshal/query.go
//     (EncodeResult, encodeData, encodeStream, encodeValue, encodeScalar)
//   - github.com/grafana/loki v3.7.9 pkg/loghttp/entry.go (entry arrays)
//   - github.com/grafana/loki v3.7.9 pkg/logql/log/labels.go
//     (parsed > structured metadata > stream label precedence)
//   - github.com/grafana/loki v3.7.9 pkg/logqlmodel/stats/stats.proto
//     (stats object shape)
//
// Handlers live in internal/query; this package only turns a plan.Result into
// bytes, so it stays independent of the executor and easy to test.
package lokiapi

import (
	"cmp"
	"encoding/json/v2"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
)

// EncodingFlagsHeader is the request header Grafana's Loki datasource sends
// to ask for a particular response encoding. Loki 3.7.9 does not echo it as a
// response header; it repeats the requested flags in data.encodingFlags.
const EncodingFlagsHeader = "X-Loki-Response-Encoding-Flags"

// FlagCategorizeLabels asks the encoder to keep stream labels separate from
// per-entry structured metadata and parsed labels. Grafana 13 sends this flag.
const FlagCategorizeLabels = "categorize-labels"

// ResponseOptions configures query response encoding. The zero value is a
// backward, uncategorized response with zeroed statistics.
type ResponseOptions struct {
	// EncodingFlags is the raw X-Loki-Response-Encoding-Flags request header.
	// It is split on commas exactly as Loki does: no trimming, duplicates
	// collapse, unknown flags are preserved and echoed in data.encodingFlags.
	EncodingFlags string
	// Direction is the query's entry order. Entries inside every encoded
	// stream are stably sorted by timestamp in this direction, and
	// uncategorized results are regrouped by their merged label set.
	Direction plan.Direction
	// Stats are the measurements the caller actually observed. Fields left
	// zero are encoded as zero; the encoder never estimates scan totals,
	// chunk/index counters or cache counters it was not given.
	Stats Stats
}

// Stats are the query measurements reported in Loki's stats object. Parkana
// only measures a subset of Loki's fields today, so the chunk, index, cache,
// ingester and store objects are encoded with Loki's shape but zero values.
// Rates are derived from the totals and ExecTime the same way Loki derives
// them (integer truncation); a zero ExecTime leaves both rates at zero.
type Stats struct {
	// ExecTime is the wall time spent executing the query.
	ExecTime time.Duration
	// QueueTime is the wall time spent waiting in a queue before execution.
	QueueTime time.Duration
	// TotalBytesProcessed is the number of bytes scanned, including
	// structured metadata bytes, if the caller measured them.
	TotalBytesProcessed int64
	// TotalLinesProcessed is the number of lines scanned.
	TotalLinesProcessed int64
	// TotalPostFilterLines is the number of lines left after query filters.
	TotalPostFilterLines int64
	// TotalStructuredMetadataBytesProcessed is the structured metadata part of
	// TotalBytesProcessed.
	TotalStructuredMetadataBytesProcessed int64
	// TotalEntriesReturned is the number of entries or samples in the result.
	TotalEntriesReturned int64
}

// EncodeQueryResult encodes res as the Loki query response JSON. It returns an
// encoding error (for example invalid UTF-8 in a line) before anything is
// written to a client.
func EncodeQueryResult(res plan.Result, opts ResponseOptions) ([]byte, error) {
	flags := parseEncodingFlags(opts.EncodingFlags)
	result, err := encodeResult(res, opts, flags)
	if err != nil {
		return nil, err
	}
	return json.Marshal(queryResponse{
		Status: "success",
		Data: queryResponseData{
			ResultType:    res.Type,
			EncodingFlags: flags,
			Result:        result,
			Stats:         encodeStats(opts.Stats),
		},
	})
}

// WriteQueryResult writes res as a 200 Loki query response. It encodes the
// whole response before writing headers, so an encoding failure becomes a
// clean 500 instead of a truncated JSON body.
func WriteQueryResult(w http.ResponseWriter, res plan.Result, opts ResponseOptions) {
	b, err := EncodeQueryResult(res, opts)
	if err != nil {
		Error(w, http.StatusInternalServerError, "parkana: encoding response: "+err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(b)
}

type queryResponse struct {
	Status string            `json:"status"`
	Data   queryResponseData `json:"data"`
}

type queryResponseData struct {
	ResultType    plan.ResultType `json:"resultType"`
	EncodingFlags []string        `json:"encodingFlags,omitempty"`
	Result        any             `json:"result"`
	Stats         statsResult     `json:"stats"`
}

type encodedStream struct {
	Stream map[string]string `json:"stream"`
	Values [][]any           `json:"values"`
}

type encodedMatrixSeries struct {
	Metric map[string]string `json:"metric"`
	Values [][]any           `json:"values"`
}

type encodedVectorSample struct {
	Metric map[string]string `json:"metric"`
	Value  []any             `json:"value"`
}

func encodeResult(res plan.Result, opts ResponseOptions, flags []string) (any, error) {
	switch res.Type {
	case plan.TypeStreams:
		return encodeStreams(res.Streams, opts, slices.Contains(flags, FlagCategorizeLabels)), nil
	case plan.TypeMatrix:
		return encodeMatrix(res.Matrix), nil
	case plan.TypeVector:
		return encodeVector(res.Vector), nil
	case plan.TypeScalar:
		return encodeSample(res.Scalar.T, res.Scalar.V), nil
	default:
		return nil, fmt.Errorf("lokiapi: unknown query result type %q", res.Type)
	}
}

func encodeStreams(streams []plan.Stream, opts ResponseOptions, categorize bool) []encodedStream {
	if categorize {
		out := make([]encodedStream, 0, len(streams))
		for _, s := range streams {
			out = append(out, encodedStream{
				Stream: s.Labels.Map(),
				Values: encodeCategorizedEntries(s.Entries, opts.Direction),
			})
		}
		sortStreams(out)
		return out
	}

	type group struct {
		labels  logs.Labels
		entries []plan.Entry
	}
	groups := make(map[string]*group, len(streams))
	for _, s := range streams {
		for _, e := range s.Entries {
			merged := mergeLabels(s.Labels, e.Meta, e.Parsed)
			key := merged.String()
			g := groups[key]
			if g == nil {
				g = &group{labels: merged}
				groups[key] = g
			}
			g.entries = append(g.entries, e)
		}
	}

	ordered := slices.Collect(maps.Values(groups))
	slices.SortFunc(ordered, func(a, b *group) int {
		return cmp.Compare(a.labels.String(), b.labels.String())
	})

	out := make([]encodedStream, 0, len(ordered))
	for _, g := range ordered {
		sortEntries(g.entries, opts.Direction)
		values := make([][]any, 0, len(g.entries))
		for _, e := range g.entries {
			values = append(values, []any{formatStreamTime(e.TS), e.Line})
		}
		out = append(out, encodedStream{Stream: g.labels.Map(), Values: values})
	}
	return out
}

func encodeCategorizedEntries(entries []plan.Entry, dir plan.Direction) [][]any {
	sorted := slices.Clone(entries)
	sortEntries(sorted, dir)
	values := make([][]any, 0, len(sorted))
	for _, e := range sorted {
		// Loki always writes the third array element in categorized mode,
		// even when both categories are empty: it is {}.
		meta := make(map[string]any, 2)
		if len(e.Meta) > 0 {
			meta["structuredMetadata"] = e.Meta.Map()
		}
		if len(e.Parsed) > 0 {
			meta["parsed"] = e.Parsed.Map()
		}
		values = append(values, []any{formatStreamTime(e.TS), e.Line, meta})
	}
	return values
}

func encodeMatrix(matrix []plan.Series) []encodedMatrixSeries {
	out := make([]encodedMatrixSeries, 0, len(matrix))
	for _, s := range matrix {
		values := make([][]any, 0, len(s.Points))
		for _, p := range s.Points {
			values = append(values, encodeSample(p.T, p.V))
		}
		out = append(out, encodedMatrixSeries{Metric: s.Metric.Map(), Values: values})
	}
	return out
}

func encodeVector(vector []plan.Sample) []encodedVectorSample {
	out := make([]encodedVectorSample, 0, len(vector))
	for _, s := range vector {
		out = append(out, encodedVectorSample{
			Metric: s.Metric.Map(),
			Value:  encodeSample(s.T, s.V),
		})
	}
	return out
}

func encodeSample(t time.Time, v float64) []any {
	return []any{sampleTimestamp(t), formatSampleValue(v)}
}

// sampleTimestamp encodes a Loki sample time: seconds as a JSON number with
// the timestamp's full nanosecond precision (Loki itself carries milliseconds).
type sampleTimestamp time.Time

func (t sampleTimestamp) MarshalJSON() ([]byte, error) {
	return formatSampleTime(time.Time(t)), nil
}

// formatStreamTime is Loki's stream entry timestamp: integer nanoseconds in a
// string, as returned by time.Time.UnixNano.
func formatStreamTime(t time.Time) string {
	return strconv.FormatInt(t.UnixNano(), 10)
}

// formatSampleTime renders t as a JSON number of seconds without a trailing
// zero fraction. It avoids float64 rounding so microsecond data stays exact.
func formatSampleTime(t time.Time) []byte {
	sec, nsec := t.Unix(), int64(t.Nanosecond())
	if nsec == 0 {
		return strconv.AppendInt(nil, sec, 10)
	}

	negative := sec < 0
	if negative {
		// time.Time.Unix floors toward negative infinity: -1ns is
		// sec=-1, nsec=999999999. Move the fraction into [0, 1s) of the
		// magnitude so the sign can be written once.
		sec = -(sec + 1)
		nsec = int64(time.Second) - nsec
	}

	var digits [9]byte
	for i := range digits {
		digits[len(digits)-1-i] = byte('0' + nsec%10)
		nsec /= 10
	}
	end := len(digits)
	for end > 1 && digits[end-1] == '0' {
		end--
	}

	b := make([]byte, 0, 1+20+1+end)
	if negative {
		b = append(b, '-')
	}
	b = strconv.AppendInt(b, sec, 10)
	b = append(b, '.')
	b = append(b, digits[:end]...)
	return b
}

// formatSampleValue is Loki's sample value string. Non-finite values follow
// Prometheus: NaN, +Inf and -Inf.
func formatSampleValue(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}

// mergeLabels merges stream labels, structured metadata and parsed labels with
// Loki's pinned precedence: parsed > structured metadata > stream. logs.New
// keeps the last duplicate, so appending in that order is enough.
func mergeLabels(stream, meta, parsed logs.Labels) logs.Labels {
	all := make([]logs.Label, 0, len(stream)+len(meta)+len(parsed))
	all = append(all, stream...)
	all = append(all, meta...)
	all = append(all, parsed...)
	return logs.New(all...)
}

// sortEntries stably orders entries by timestamp in dir. Equal timestamps keep
// the order the executor produced, which plan.Source leaves unspecified and
// Loki likewise does not promise.
func sortEntries(entries []plan.Entry, dir plan.Direction) {
	slices.SortStableFunc(entries, func(a, b plan.Entry) int {
		if dir == plan.Forward {
			return a.TS.Compare(b.TS)
		}
		return b.TS.Compare(a.TS)
	})
}

// sortStreams matches Loki's readStreams output: streams sorted by their
// canonical label string.
func sortStreams(streams []encodedStream) {
	slices.SortStableFunc(streams, func(a, b encodedStream) int {
		return cmp.Compare(labelsKey(a.Stream), labelsKey(b.Stream))
	})
}

func labelsKey(m map[string]string) string {
	return logs.FromMap(m).String()
}

// parseEncodingFlags splits the header the way Loki 3.7.9 does: on commas,
// without trimming, deduplicated. It returns nil for an absent header so the
// engine omits data.encodingFlags entirely. Unknown flags are preserved.
func parseEncodingFlags(raw string) []string {
	if raw == "" {
		return nil
	}
	flags := make(map[string]struct{})
	for flag := range strings.SplitSeq(raw, ",") {
		flags[flag] = struct{}{}
	}
	return slices.Sorted(maps.Keys(flags))
}

// The types below reproduce Loki 3.7.9's stats.Result JSON shape. Parkana
// reports measurements only in Summary; every other counter is zero because it
// was not measured, not estimated.
type statsResult struct {
	Summary  statsSummary  `json:"summary"`
	Querier  statsQuerier  `json:"querier"`
	Ingester statsIngester `json:"ingester"`
	Cache    statsCaches   `json:"cache"`
	Index    statsIndex    `json:"index"`
}

type statsSummary struct {
	BytesProcessedPerSecond               int64   `json:"bytesProcessedPerSecond"`
	LinesProcessedPerSecond               int64   `json:"linesProcessedPerSecond"`
	TotalBytesProcessed                   int64   `json:"totalBytesProcessed"`
	TotalLinesProcessed                   int64   `json:"totalLinesProcessed"`
	ExecTime                              float64 `json:"execTime"`
	QueueTime                             float64 `json:"queueTime"`
	Subqueries                            int64   `json:"subqueries"`
	TotalEntriesReturned                  int64   `json:"totalEntriesReturned"`
	Splits                                int64   `json:"splits"`
	Shards                                int64   `json:"shards"`
	TotalPostFilterLines                  int64   `json:"totalPostFilterLines"`
	TotalStructuredMetadataBytesProcessed int64   `json:"totalStructuredMetadataBytesProcessed"`
}

type statsQuerier struct {
	Store           statsStore `json:"store"`
	QuerierExecTime float64    `json:"querierExecTime"`
}

type statsIngester struct {
	TotalReached       int32      `json:"totalReached"`
	TotalChunksMatched int64      `json:"totalChunksMatched"`
	TotalBatches       int64      `json:"totalBatches"`
	TotalLinesSent     int64      `json:"totalLinesSent"`
	Store              statsStore `json:"store"`
	RecvWaitTime       float64    `json:"recvWaitTime,omitzero"`
}

type statsStore struct {
	TotalChunksRef               int64        `json:"totalChunksRef"`
	TotalChunksDownloaded        int64        `json:"totalChunksDownloaded"`
	ChunksDownloadTime           int64        `json:"chunksDownloadTime"`
	QueryReferencedStructured    bool         `json:"queryReferencedStructuredMetadata"`
	QueryUsedV2Engine            bool         `json:"queryUsedV2Engine"`
	Chunk                        statsChunk   `json:"chunk"`
	ChunkRefsFetchTime           int64        `json:"chunkRefsFetchTime"`
	CongestionControlLatency     int64        `json:"congestionControlLatency"`
	PipelineWrapperFilteredLines int64        `json:"pipelineWrapperFilteredLines"`
	Dataobj                      statsDataobj `json:"dataobj"`
}

type statsChunk struct {
	HeadChunkBytes                      int64 `json:"headChunkBytes"`
	HeadChunkLines                      int64 `json:"headChunkLines"`
	DecompressedBytes                   int64 `json:"decompressedBytes"`
	DecompressedLines                   int64 `json:"decompressedLines"`
	CompressedBytes                     int64 `json:"compressedBytes"`
	TotalDuplicates                     int64 `json:"totalDuplicates"`
	PostFilterLines                     int64 `json:"postFilterLines"`
	HeadChunkStructuredMetadataBytes    int64 `json:"headChunkStructuredMetadataBytes"`
	DecompressedStructuredMetadataBytes int64 `json:"decompressedStructuredMetadataBytes"`
}

type statsDataobj struct {
	PrePredicateDecompressedRows                    int64 `json:"prePredicateDecompressedRows"`
	PrePredicateDecompressedBytes                   int64 `json:"prePredicateDecompressedBytes"`
	PrePredicateDecompressedStructuredMetadataBytes int64 `json:"prePredicateDecompressedStructuredMetadataBytes"`
	PostPredicateRows                               int64 `json:"postPredicateRows"`
	PostPredicateDecompressedBytes                  int64 `json:"postPredicateDecompressedBytes"`
	PostPredicateStructuredMetadataBytes            int64 `json:"postPredicateStructuredMetadataBytes"`
	PostFilterRows                                  int64 `json:"postFilterRows"`
	PagesScanned                                    int64 `json:"pagesScanned"`
	PagesDownloaded                                 int64 `json:"pagesDownloaded"`
	PagesDownloadedBytes                            int64 `json:"pagesDownloadedBytes"`
	PageBatches                                     int64 `json:"pageBatches"`
	TotalRowsAvailable                              int64 `json:"totalRowsAvailable"`
	TotalPageDownloadTime                           int64 `json:"totalPageDownloadTime"`
}

type statsCaches struct {
	Chunk               statsCache `json:"chunk"`
	Index               statsCache `json:"index"`
	Result              statsCache `json:"result"`
	StatsResult         statsCache `json:"statsResult"`
	VolumeResult        statsCache `json:"volumeResult"`
	SeriesResult        statsCache `json:"seriesResult"`
	LabelResult         statsCache `json:"labelResult"`
	InstantMetricResult statsCache `json:"instantMetricResult"`
}

type statsCache struct {
	EntriesFound      int32 `json:"entriesFound"`
	EntriesRequested  int32 `json:"entriesRequested"`
	EntriesStored     int32 `json:"entriesStored"`
	BytesReceived     int64 `json:"bytesReceived"`
	BytesSent         int64 `json:"bytesSent"`
	Requests          int32 `json:"requests"`
	DownloadTime      int64 `json:"downloadTime"`
	QueryLengthServed int64 `json:"queryLengthServed"`
}

type statsIndex struct {
	TotalChunks         int64   `json:"totalChunks"`
	PostFilterChunks    int64   `json:"postFilterChunks"`
	ShardsDuration      int64   `json:"shardsDuration"`
	UsedBloomFilters    bool    `json:"usedBloomFilters"`
	TotalStreams        int64   `json:"totalStreams"`
	ChunkRefsLookupTime float64 `json:"chunkRefsLookupTime"`
	BloomFilterTime     float64 `json:"bloomFilterTime"`
}

func encodeStats(s Stats) statsResult {
	summary := statsSummary{
		TotalBytesProcessed:                   s.TotalBytesProcessed,
		TotalLinesProcessed:                   s.TotalLinesProcessed,
		TotalPostFilterLines:                  s.TotalPostFilterLines,
		TotalStructuredMetadataBytesProcessed: s.TotalStructuredMetadataBytesProcessed,
		TotalEntriesReturned:                  s.TotalEntriesReturned,
		ExecTime:                              s.ExecTime.Seconds(),
		QueueTime:                             s.QueueTime.Seconds(),
	}
	if s.ExecTime != 0 {
		summary.BytesProcessedPerSecond = int64(float64(s.TotalBytesProcessed) / s.ExecTime.Seconds())
		summary.LinesProcessedPerSecond = int64(float64(s.TotalLinesProcessed) / s.ExecTime.Seconds())
	}
	return statsResult{Summary: summary}
}

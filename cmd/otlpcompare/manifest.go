package main

import (
	"bytes"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// toolName identifies artifacts written by this command.
const toolName = "otlpcompare"

// fixtureManifest is the contract between send and compare. It records the
// fixture identity, the exact fixture plan that was sent, and the time window
// the fixed query set uses. compare never invents fixture counts: it requires
// them from this file.
type fixtureManifest struct {
	Tool           string                `json:"tool"`
	FixtureID      string                `json:"fixtureId"`
	FixtureVersion int                   `json:"fixtureVersion"`
	PDataModule    string                `json:"pdataModule"`
	PDataVersion   string                `json:"pdataVersion"`
	RunID          string                `json:"runId"`
	Namespace      string                `json:"namespace"`
	CodecMode      string                `json:"codecMode"`
	Gzip           bool                  `json:"gzip"`
	Target         string                `json:"target"`
	Anchor         string                `json:"anchor"`
	WindowStart    string                `json:"windowStart"`
	WindowEnd      string                `json:"windowEnd"`
	FirstTimestamp string                `json:"firstTimestamp"`
	LastTimestamp  string                `json:"lastTimestamp"`
	TotalRecords   int                   `json:"totalRecords"`
	Cases          []fixtureManifestCase `json:"cases"`
}

// fixtureManifestCase is one sent fixture export and its expected entry count.
type fixtureManifestCase struct {
	Name       string   `json:"name"`
	Instance   string   `json:"instance"`
	Codec      string   `json:"codec"`
	Gzip       bool     `json:"gzip"`
	Records    int      `json:"records"`
	Timestamps []string `json:"timestamps"`
}

// manifestTimes is the parsed time information the query set needs.
type manifestTimes struct {
	anchor time.Time
	window timeBounds
	first  time.Time
	last   time.Time
}

// buildManifest renders the manifest for a plan. codecMode selects the payload
// codecs and gzip records the transport encoding used for every request.
func (p fixturePlan) buildManifest(target, codecMode string, gzip bool) fixtureManifest {
	m := fixtureManifest{
		Tool:           toolName,
		FixtureID:      fixtureID,
		FixtureVersion: fixtureVersion,
		PDataModule:    pdataModule,
		PDataVersion:   pdataVersion,
		RunID:          p.runID,
		Namespace:      p.namespace,
		CodecMode:      codecMode,
		Gzip:           gzip,
		Target:         target,
		Anchor:         formatTime(p.anchor),
		WindowStart:    formatTime(p.windowStart()),
		WindowEnd:      formatTime(p.windowEnd()),
		FirstTimestamp: formatTime(p.first),
		LastTimestamp:  formatTime(p.last),
		TotalRecords:   p.total,
		Cases:          make([]fixtureManifestCase, 0, len(p.cases)),
	}
	for i, c := range p.cases {
		timestamps := make([]string, 0, len(c.records))
		for _, rec := range c.records {
			timestamps = append(timestamps, formatTime(rec.timestamp))
		}
		m.Cases = append(m.Cases, fixtureManifestCase{
			Name:       c.name,
			Instance:   c.instance,
			Codec:      codecFor(codecMode, i),
			Gzip:       gzip,
			Records:    len(c.records),
			Timestamps: timestamps,
		})
	}
	return m
}

// parse validates the manifest and parses its timestamps.
func (m fixtureManifest) parse() (manifestTimes, error) {
	if m.Tool != toolName {
		return manifestTimes{}, fmt.Errorf("manifest tool is %q, want %q", m.Tool, toolName)
	}
	if m.FixtureID != fixtureID || m.FixtureVersion != fixtureVersion {
		return manifestTimes{}, fmt.Errorf("manifest fixture is %s v%d, this binary implements %s v%d",
			m.FixtureID, m.FixtureVersion, fixtureID, fixtureVersion)
	}
	if m.PDataModule != pdataModule || m.PDataVersion != pdataVersion {
		return manifestTimes{}, fmt.Errorf("manifest pdata pin is %s@%s, this binary implements %s@%s",
			m.PDataModule, m.PDataVersion, pdataModule, pdataVersion)
	}
	if m.RunID == "" {
		return manifestTimes{}, errors.New("manifest has no runId")
	}
	if m.Namespace == "" {
		return manifestTimes{}, errors.New("manifest has no namespace")
	}
	if m.Target == "" {
		return manifestTimes{}, errors.New("manifest has no target")
	}
	if !validCodec(m.CodecMode) {
		return manifestTimes{}, fmt.Errorf("manifest codecMode %q is invalid", m.CodecMode)
	}
	if len(m.Cases) == 0 {
		return manifestTimes{}, errors.New("manifest has no cases")
	}
	parse := func(name, raw string) (time.Time, error) {
		t, err := parseTime(raw)
		if err != nil {
			return time.Time{}, fmt.Errorf("manifest %s: %w", name, err)
		}
		return t, nil
	}
	anchor, err := parse("anchor", m.Anchor)
	if err != nil {
		return manifestTimes{}, err
	}
	windowStart, err := parse("windowStart", m.WindowStart)
	if err != nil {
		return manifestTimes{}, err
	}
	windowEnd, err := parse("windowEnd", m.WindowEnd)
	if err != nil {
		return manifestTimes{}, err
	}
	first, err := parse("firstTimestamp", m.FirstTimestamp)
	if err != nil {
		return manifestTimes{}, err
	}
	last, err := parse("lastTimestamp", m.LastTimestamp)
	if err != nil {
		return manifestTimes{}, err
	}
	if !windowStart.Before(first) {
		return manifestTimes{}, fmt.Errorf("manifest windowStart %s is not before firstTimestamp %s",
			m.WindowStart, m.FirstTimestamp)
	}
	if !last.Before(windowEnd) {
		return manifestTimes{}, fmt.Errorf("manifest windowEnd %s is not after lastTimestamp %s",
			m.WindowEnd, m.LastTimestamp)
	}
	if last.Before(first) {
		return manifestTimes{}, fmt.Errorf("manifest lastTimestamp %s is before firstTimestamp %s",
			m.LastTimestamp, m.FirstTimestamp)
	}
	if m.TotalRecords <= 0 {
		return manifestTimes{}, fmt.Errorf("manifest totalRecords must be positive, got %d", m.TotalRecords)
	}

	seen := make(map[string]struct{}, len(m.Cases))
	sum := 0
	earliest, latest := time.Time{}, time.Time{}
	for i, c := range m.Cases {
		if c.Name == "" {
			return manifestTimes{}, fmt.Errorf("manifest case %d has no name", i)
		}
		if _, ok := seen[c.Name]; ok {
			return manifestTimes{}, fmt.Errorf("manifest has duplicate case %q", c.Name)
		}
		seen[c.Name] = struct{}{}
		if c.Instance == "" {
			return manifestTimes{}, fmt.Errorf("manifest case %q has no instance", c.Name)
		}
		if c.Codec != codecProtobuf && c.Codec != codecJSON {
			return manifestTimes{}, fmt.Errorf("manifest case %q has invalid codec %q", c.Name, c.Codec)
		}
		if c.Records <= 0 {
			return manifestTimes{}, fmt.Errorf("manifest case %q has %d records", c.Name, c.Records)
		}
		if len(c.Timestamps) != c.Records {
			return manifestTimes{}, fmt.Errorf("manifest case %q has %d timestamps for %d records",
				c.Name, len(c.Timestamps), c.Records)
		}
		sum += c.Records
		for j, raw := range c.Timestamps {
			ts, err := parseTime(raw)
			if err != nil {
				return manifestTimes{}, fmt.Errorf("manifest case %q timestamp %d: %w", c.Name, j, err)
			}
			if ts.Before(windowStart) || !ts.Before(windowEnd) {
				return manifestTimes{}, fmt.Errorf("manifest case %q timestamp %d %s is outside [%s, %s)",
					c.Name, j, raw, m.WindowStart, m.WindowEnd)
			}
			if j > 0 {
				prev, err := parseTime(c.Timestamps[j-1])
				if err != nil {
					return manifestTimes{}, err
				}
				if !prev.Before(ts) {
					return manifestTimes{}, fmt.Errorf("manifest case %q timestamps are not increasing", c.Name)
				}
			}
			if earliest.IsZero() || ts.Before(earliest) {
				earliest = ts
			}
			if latest.IsZero() || ts.After(latest) {
				latest = ts
			}
		}
	}
	if sum != m.TotalRecords {
		return manifestTimes{}, fmt.Errorf("manifest totalRecords is %d but case records sum to %d", m.TotalRecords, sum)
	}
	if !earliest.Equal(first) {
		return manifestTimes{}, fmt.Errorf("manifest firstTimestamp is %s but the earliest case timestamp is %s",
			formatTime(first), formatTime(earliest))
	}
	if !latest.Equal(last) {
		return manifestTimes{}, fmt.Errorf("manifest lastTimestamp is %s but the latest case timestamp is %s",
			formatTime(last), formatTime(latest))
	}
	return manifestTimes{
		anchor: anchor,
		window: timeBounds{start: windowStart, end: windowEnd},
		first:  first,
		last:   last,
	}, nil
}

// writeManifest writes the manifest through a same-directory temp file and
// rename, so a concurrent reader never observes a partial artifact.
func writeManifest(path string, m fixtureManifest) error {
	var buf bytes.Buffer
	enc := jsontext.NewEncoder(&buf, jsontext.WithIndent("  "))
	if err := json.MarshalEncode(enc, m); err != nil {
		return fmt.Errorf("encode manifest: %w", err)
	}
	buf.WriteByte('\n')
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".otlpcompare-manifest-*")
	if err != nil {
		return fmt.Errorf("create manifest temp file: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := func() {
		tmp.Close()
		os.Remove(tmpName)
	}
	if _, err := tmp.Write(buf.Bytes()); err != nil {
		cleanup()
		return fmt.Errorf("write manifest temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("close manifest temp file: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("rename manifest: %w", err)
	}
	return nil
}

// readManifest loads and validates a manifest.
func readManifest(path string) (fixtureManifest, manifestTimes, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return fixtureManifest{}, manifestTimes{}, fmt.Errorf("read manifest %s: %w", path, err)
	}
	var m fixtureManifest
	if err := json.Unmarshal(data, &m); err != nil {
		return fixtureManifest{}, manifestTimes{}, fmt.Errorf("parse manifest %s: %w", path, err)
	}
	times, err := m.parse()
	if err != nil {
		return fixtureManifest{}, manifestTimes{}, fmt.Errorf("manifest %s: %w", path, err)
	}
	return m, times, nil
}

// checkManifestMatchesPlan requires the manifest to describe exactly the
// compiled corpus for its anchor. A different binary, reordered cases or
// edited counts cannot be compared under stale expectations.
func checkManifestMatchesPlan(m fixtureManifest, plan fixturePlan) error {
	want, err := json.Marshal(plan.buildManifest(m.Target, m.CodecMode, m.Gzip))
	if err != nil {
		return fmt.Errorf("encode expected manifest: %w", err)
	}
	got, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("encode manifest: %w", err)
	}
	if !bytes.Equal(want, got) {
		return errors.New("manifest does not match the compiled fixture plan; regenerate it with the same otlpcompare binary that sent the fixtures")
	}
	return nil
}

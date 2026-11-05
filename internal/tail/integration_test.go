package tail_test

// This file is the end-to-end acceptance test for the public live tail: it
// runs the real ingester factory (WAL, memtable, push handler, private NDJSON
// tail endpoint) and the real querier factory (public WebSocket handler) as
// two roles on one HTTP server, pushes a row over the real Loki push API and
// reads it back through a real WebSocket client. The row is only published
// after the ingester's WAL durability step, so receiving it proves the whole
// path. It needs no external service and runs in the normal test suite.

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestLiveTailEndToEndAfterWALDurability(t *testing.T) {
	stack := startTailStack(t)
	ctx := t.Context()

	// Subscribe first: live tail has no history, so the push must land after
	// the subscription exists.
	dialCtx, dialCancel := context.WithTimeout(ctx, 5*time.Second)
	conn, _, err := websocket.Dial(dialCtx, stack.wsBase+"/loki/api/v1/tail?query="+url.QueryEscape(`{job="api"}`), nil)
	dialCancel()
	if err != nil {
		t.Fatalf("dial tail: %v", err)
	}
	defer conn.CloseNow()
	// Wait for both sides of the subscription: the querier's public
	// subscriber gauge increments before its private reads are ready, so the
	// ingester-side subscriber gauge must also show the subscription before a
	// push can be expected to be tailed. No sleep is used as readiness proof.
	waitMetricAtLeast(t, stack.reg, "parkana_tail_subscribers", 1)
	waitMetricAtLeast(t, stack.reg, "parkana_ingest_tail_subscribers", 1)

	// Push through the real Loki push endpoint. The ingester answers only
	// after the WAL fsync, and its tail hub publishes after that, so a row
	// read below is durable.
	ts := time.Now().Add(-time.Millisecond).UnixNano()
	body := fmt.Sprintf(`{"streams":[{"stream":{"job":"api"},"values":[["%d","integration-line"]]}]}`, ts)
	resp, err := http.Post(stack.base+"/loki/api/v1/push", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("push: %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		t.Fatalf("push status = %d", resp.StatusCode)
	}

	deadline, stopDeadline := context.WithTimeout(ctx, 10*time.Second)
	defer stopDeadline()
	for {
		_, data, err := conn.Read(deadline)
		if err != nil {
			t.Fatalf("read tail: %v", err)
		}
		var msg struct {
			Streams []struct {
				Stream map[string]string  `json:"stream"`
				Values [][]jsontext.Value `json:"values"`
			} `json:"streams"`
		}
		if err := json.Unmarshal(data, &msg); err != nil {
			t.Fatalf("decode %s: %v", data, err)
		}
		for _, stream := range msg.Streams {
			for _, value := range stream.Values {
				if len(value) < 2 {
					continue
				}
				var line string
				if err := json.Unmarshal(value[1], &line); err != nil || line != "integration-line" {
					continue
				}
				if stream.Stream["job"] != "api" {
					t.Fatalf("stream labels = %v", stream.Stream)
				}
				var gotTS string
				if err := json.Unmarshal(value[0], &gotTS); err != nil {
					t.Fatalf("timestamp %s: %v", value[0], err)
				}
				if want := strconv.FormatInt(time.UnixMicro(time.Unix(0, ts).UnixMicro()).UnixNano(), 10); gotTS != want {
					t.Fatalf("timestamp = %s, want microsecond-grid %s", gotTS, want)
				}
				return
			}
		}
	}
}

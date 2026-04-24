package usage

import (
	"context"
	"testing"
	"time"

	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

func TestNormalizeUsageRecordKeyIgnoresLatency(t *testing.T) {
	requestedAt := time.Date(2026, 4, 24, 9, 30, 0, 0, time.UTC)
	base := coreusage.Record{
		APIKey:      "test-key",
		Model:       "gpt-5.4",
		RequestedAt: requestedAt,
		Source:      "codex",
		AuthIndex:   "1",
		Detail: coreusage.Detail{
			InputTokens:  10,
			OutputTokens: 20,
			TotalTokens:  30,
		},
	}
	first := base
	first.Latency = 1500 * time.Millisecond
	second := base
	second.Latency = 2500 * time.Millisecond

	firstRecord := normalizeUsageRecord(context.Background(), first)
	secondRecord := normalizeUsageRecord(context.Background(), second)

	if firstRecord.RecordKey == "" {
		t.Fatal("record key is empty")
	}
	if firstRecord.RecordKey != secondRecord.RecordKey {
		t.Fatalf("record keys differ for latency-only change: %s != %s", firstRecord.RecordKey, secondRecord.RecordKey)
	}
	if firstRecord.APIKey == base.APIKey {
		t.Fatalf("api key label leaked raw key %q", firstRecord.APIKey)
	}
	if firstRecord.APIKeyHash == "" {
		t.Fatal("api key hash is empty")
	}
}

func TestUsageDisplayLabelKeepsNonSecretIdentifiers(t *testing.T) {
	if got := usageDisplayLabel("GET /v1/models", false); got != "GET /v1/models" {
		t.Fatalf("route label = %q, want original route", got)
	}
	if got := safeStoredAPIKeyLabel("openai", hashUsageValue("openai")); got != "openai" {
		t.Fatalf("provider label = %q, want openai", got)
	}
}

func TestSnapshotFromUsageRecordsPreservesManagementShape(t *testing.T) {
	requestedAt := time.Date(2026, 4, 24, 9, 30, 0, 0, time.UTC)
	snapshot := snapshotFromUsageRecords([]UsageRecord{
		{
			APIKey:      "key-a",
			Model:       "gpt-5.4",
			RequestedAt: requestedAt,
			LatencyMs:   100,
			Tokens: TokenStats{
				InputTokens:  10,
				OutputTokens: 20,
				TotalTokens:  30,
			},
		},
		{
			APIKey:      "key-a",
			Model:       "gpt-5.4",
			RequestedAt: requestedAt.Add(time.Hour),
			LatencyMs:   200,
			Failed:      true,
			Tokens: TokenStats{
				InputTokens:  5,
				OutputTokens: 5,
				TotalTokens:  10,
			},
		},
	})

	if snapshot.TotalRequests != 2 {
		t.Fatalf("total requests = %d, want 2", snapshot.TotalRequests)
	}
	if snapshot.SuccessCount != 1 || snapshot.FailureCount != 1 {
		t.Fatalf("success/failure = %d/%d, want 1/1", snapshot.SuccessCount, snapshot.FailureCount)
	}
	if snapshot.TotalTokens != 40 {
		t.Fatalf("total tokens = %d, want 40", snapshot.TotalTokens)
	}
	apiSnapshot := snapshot.APIs["key-a"]
	modelSnapshot := apiSnapshot.Models["gpt-5.4"]
	if modelSnapshot.TotalRequests != 2 || len(modelSnapshot.Details) != 2 {
		t.Fatalf("model snapshot = %+v, want 2 requests and 2 details", modelSnapshot)
	}
	if snapshot.RequestsByHour["09"] != 1 || snapshot.RequestsByHour["10"] != 1 {
		t.Fatalf("requests by hour = %+v, want 09=1 and 10=1", snapshot.RequestsByHour)
	}
}

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

func TestNormalizeUsageRecordMasksPersistentSource(t *testing.T) {
	record := normalizeUsageRecord(context.Background(), coreusage.Record{
		APIKey:      "sk-client-secret",
		Model:       "gpt-5.4",
		RequestedAt: time.Date(2026, 4, 24, 9, 30, 0, 0, time.UTC),
		Source:      "sk-upstream-secret",
		Detail:      coreusage.Detail{InputTokens: 1, OutputTokens: 2, TotalTokens: 3},
	})

	if record.Source == "sk-upstream-secret" {
		t.Fatalf("persistent source label leaked raw source %q", record.Source)
	}
	if record.RecordKey == "" {
		t.Fatal("record key is empty")
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

func TestNormalizeStoredUsageRecordMasksSensitiveSource(t *testing.T) {
	record := normalizeStoredUsageRecord(UsageRecord{
		APIKey: "sk-client-secret",
		Source: "sk-upstream-secret",
		Model:  "gpt-5.4",
	})

	if record.APIKey == "sk-client-secret" {
		t.Fatalf("api key label leaked raw key %q", record.APIKey)
	}
	if record.Source == "sk-upstream-secret" {
		t.Fatalf("source label leaked raw key %q", record.Source)
	}
	if record.Source == "" || record.APIKeyHash == "" {
		t.Fatalf("normalized record missing source/hash: %+v", record)
	}
}

func TestNormalizeStoredUsageRecordRekeysAfterSanitizing(t *testing.T) {
	requestedAt := time.Date(2026, 4, 24, 9, 30, 0, 0, time.UTC)
	tokens := TokenStats{InputTokens: 10, OutputTokens: 20, TotalTokens: 30}
	staleRawKey := stableUsageRecordKey("sk-client-secret", "gpt-5.4", RequestDetail{
		Timestamp: requestedAt,
		Source:    "sk-upstream-secret",
		AuthIndex: "1",
		Tokens:    tokens,
	})

	record := normalizeStoredUsageRecord(UsageRecord{
		RecordKey:   staleRawKey,
		APIKey:      "sk-client-secret",
		Source:      "sk-upstream-secret",
		AuthIndex:   "1",
		Model:       "gpt-5.4",
		RequestedAt: requestedAt,
		Tokens:      tokens,
	})

	if record.RecordKey == staleRawKey {
		t.Fatalf("record key still derived from raw identifiers: %s", record.RecordKey)
	}
	if record.RecordKey != storedUsageRecordKey(record) {
		t.Fatalf("record key = %s, want sanitized key %s", record.RecordKey, storedUsageRecordKey(record))
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

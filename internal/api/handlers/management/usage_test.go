package management

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/usage"
)

type fakeUsageStore struct {
	snapshot     usage.StatisticsSnapshot
	snapshotErr  error
	importResult usage.MergeResult
	importErr    error
	imported     bool
}

func (f *fakeUsageStore) EnsureSchema(context.Context) error { return nil }

func (f *fakeUsageStore) Record(context.Context, usage.UsageRecord) error { return nil }

func (f *fakeUsageStore) Snapshot(context.Context) (usage.StatisticsSnapshot, error) {
	if f.snapshotErr != nil {
		return usage.StatisticsSnapshot{}, f.snapshotErr
	}
	return f.snapshot, nil
}

func (f *fakeUsageStore) ImportSnapshot(context.Context, usage.StatisticsSnapshot) (usage.MergeResult, error) {
	f.imported = true
	if f.importErr != nil {
		return usage.MergeResult{}, f.importErr
	}
	return f.importResult, nil
}

func TestGetUsageStatisticsPrefersPersistentStore(t *testing.T) {
	h := NewHandlerWithoutConfigFilePath(&config.Config{}, nil)
	memoryStats := usage.NewRequestStatistics()
	memoryStats.MergeSnapshot(snapshotWithOneDetail("memory-key", "gpt-5.4"))
	h.SetUsageStatistics(memoryStats)
	h.SetUsageStore(&fakeUsageStore{
		snapshot: usage.StatisticsSnapshot{
			TotalRequests: 2,
			FailureCount:  1,
			APIs:          map[string]usage.APISnapshot{},
		},
	})

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/v0/management/usage", nil)
	h.GetUsageStatistics(c)

	var response struct {
		Usage usage.StatisticsSnapshot `json:"usage"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if response.Usage.TotalRequests != 2 || response.Usage.FailureCount != 1 {
		t.Fatalf("usage snapshot = %+v, want persistent snapshot", response.Usage)
	}
}

func TestGetUsageStatisticsFallsBackToMemory(t *testing.T) {
	h := NewHandlerWithoutConfigFilePath(&config.Config{}, nil)
	memoryStats := usage.NewRequestStatistics()
	memoryStats.MergeSnapshot(snapshotWithOneDetail("memory-key", "gpt-5.4"))
	h.SetUsageStatistics(memoryStats)
	h.SetUsageStore(&fakeUsageStore{snapshotErr: errors.New("db unavailable")})

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/v0/management/usage", nil)
	h.GetUsageStatistics(c)

	var response struct {
		Usage usage.StatisticsSnapshot `json:"usage"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if response.Usage.TotalRequests != 1 || response.Usage.APIs["memory-key"].TotalRequests != 1 {
		t.Fatalf("usage snapshot = %+v, want memory fallback", response.Usage)
	}
}

func TestImportUsageSnapshotReturnsPersistentResultAndWarmsMemory(t *testing.T) {
	h := NewHandlerWithoutConfigFilePath(&config.Config{}, nil)
	memoryStats := usage.NewRequestStatistics()
	h.SetUsageStatistics(memoryStats)
	store := &fakeUsageStore{importResult: usage.MergeResult{Added: 7, Skipped: 3}}
	h.SetUsageStore(store)

	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v0/management/usage/import", bytes.NewReader(nil))
	result := h.importUsageSnapshot(c, snapshotWithOneDetail("import-key", "gpt-5.4"))

	if !store.imported {
		t.Fatal("persistent store was not called")
	}
	if result.Added != 7 || result.Skipped != 3 {
		t.Fatalf("import result = %+v, want persistent result", result)
	}
	if got := memoryStats.Snapshot().TotalRequests; got != 1 {
		t.Fatalf("memory total requests = %d, want 1", got)
	}
}

func TestImportUsageSnapshotFallsBackToMemoryResult(t *testing.T) {
	h := NewHandlerWithoutConfigFilePath(&config.Config{}, nil)
	memoryStats := usage.NewRequestStatistics()
	h.SetUsageStatistics(memoryStats)
	h.SetUsageStore(&fakeUsageStore{importErr: errors.New("db unavailable")})

	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v0/management/usage/import", bytes.NewReader(nil))
	result := h.importUsageSnapshot(c, snapshotWithOneDetail("import-key", "gpt-5.4"))

	if result.Added != 1 || result.Skipped != 0 {
		t.Fatalf("import result = %+v, want memory fallback result", result)
	}
}

func snapshotWithOneDetail(apiName, modelName string) usage.StatisticsSnapshot {
	return usage.StatisticsSnapshot{
		APIs: map[string]usage.APISnapshot{
			apiName: {
				Models: map[string]usage.ModelSnapshot{
					modelName: {
						Details: []usage.RequestDetail{{
							Timestamp: time.Date(2026, 4, 24, 9, 30, 0, 0, time.UTC),
							Tokens: usage.TokenStats{
								InputTokens:  1,
								OutputTokens: 2,
								TotalTokens:  3,
							},
						}},
					},
				},
			},
		},
	}
}

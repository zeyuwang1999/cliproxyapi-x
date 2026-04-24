package usage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"sync"
	"time"

	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
	log "github.com/sirupsen/logrus"
)

const defaultPersistenceQueueSize = 2048

// StatisticsStore persists request-level usage records and can reconstruct the
// management-page aggregate shape from those records.
type StatisticsStore interface {
	EnsureSchema(ctx context.Context) error
	Record(ctx context.Context, record UsageRecord) error
	Snapshot(ctx context.Context) (StatisticsSnapshot, error)
	ImportSnapshot(ctx context.Context, snapshot StatisticsSnapshot) (MergeResult, error)
}

// UsageRecord is the normalized, persistence-friendly representation of one
// usage event.
type UsageRecord struct {
	RecordKey   string
	APIKey      string
	APIKeyHash  string
	Provider    string
	Model       string
	Source      string
	AuthID      string
	AuthIndex   string
	RequestedAt time.Time
	LatencyMs   int64
	Failed      bool
	Tokens      TokenStats
}

type persistentPlugin struct {
	queue chan UsageRecord
	once  sync.Once

	storeMu sync.RWMutex
	store   StatisticsStore
}

var defaultPersistentPlugin = newPersistentPlugin(defaultPersistenceQueueSize)

func newPersistentPlugin(queueSize int) *persistentPlugin {
	if queueSize <= 0 {
		queueSize = defaultPersistenceQueueSize
	}
	return &persistentPlugin{queue: make(chan UsageRecord, queueSize)}
}

// SetStatisticsStore configures the optional durable usage store. Passing nil
// disables durable writes while leaving in-memory statistics untouched.
func SetStatisticsStore(store StatisticsStore) {
	defaultPersistentPlugin.setStore(store)
}

// GetStatisticsStore returns the currently configured durable usage store.
func GetStatisticsStore() StatisticsStore {
	return defaultPersistentPlugin.getStore()
}

func (p *persistentPlugin) setStore(store StatisticsStore) {
	if p == nil {
		return
	}
	p.storeMu.Lock()
	p.store = store
	p.storeMu.Unlock()
	if store != nil {
		p.once.Do(func() { go p.run() })
	}
}

func (p *persistentPlugin) getStore() StatisticsStore {
	if p == nil {
		return nil
	}
	p.storeMu.RLock()
	defer p.storeMu.RUnlock()
	return p.store
}

func (p *persistentPlugin) HandleUsage(ctx context.Context, record coreusage.Record) {
	if !statisticsEnabled.Load() {
		return
	}
	if p == nil || p.getStore() == nil {
		return
	}
	normalized := normalizeUsageRecord(ctx, record)
	select {
	case p.queue <- normalized:
	default:
		log.Warn("usage persistence queue is full; dropping usage record")
	}
}

func (p *persistentPlugin) run() {
	for record := range p.queue {
		store := p.getStore()
		if store == nil {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := store.Record(ctx, record)
		cancel()
		if err != nil {
			log.WithError(err).Warn("failed to persist usage record")
		}
	}
}

func normalizeUsageRecord(ctx context.Context, record coreusage.Record) UsageRecord {
	timestamp := record.RequestedAt
	if timestamp.IsZero() {
		timestamp = time.Now()
	}
	tokens := normaliseDetail(record.Detail)
	statsKey := record.APIKey
	sensitiveStatsKey := statsKey != ""
	if statsKey == "" {
		statsKey = resolveAPIIdentifier(ctx, record)
	}
	failed := record.Failed
	if !failed {
		failed = !resolveSuccess(ctx)
	}
	modelName := record.Model
	if modelName == "" {
		modelName = "unknown"
	}
	detail := RequestDetail{
		Timestamp: timestamp,
		LatencyMs: normaliseLatency(record.Latency),
		Source:    record.Source,
		AuthIndex: record.AuthIndex,
		Tokens:    tokens,
		Failed:    failed,
	}
	return UsageRecord{
		RecordKey:   stableUsageRecordKey(statsKey, modelName, detail),
		APIKey:      usageDisplayLabel(statsKey, sensitiveStatsKey),
		APIKeyHash:  hashUsageValue(statsKey),
		Provider:    record.Provider,
		Model:       modelName,
		Source:      record.Source,
		AuthID:      record.AuthID,
		AuthIndex:   record.AuthIndex,
		RequestedAt: timestamp,
		LatencyMs:   detail.LatencyMs,
		Failed:      failed,
		Tokens:      tokens,
	}
}

func stableUsageRecordKey(apiName, modelName string, detail RequestDetail) string {
	sum := sha256.Sum256([]byte(dedupKey(apiName, modelName, detail)))
	return hex.EncodeToString(sum[:])
}

func hashUsageValue(value string) string {
	if value == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func usageDisplayLabel(value string, sensitive bool) string {
	return usageDisplayLabelWithHash(value, hashUsageValue(value), sensitive)
}

func usageDisplayLabelWithHash(value, hash string, sensitive bool) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	if !sensitive || isUsageDisplayLabel(trimmed) {
		return trimmed
	}
	hashPrefix := strings.TrimSpace(hash)
	if hashPrefix == "" {
		hashPrefix = hashUsageValue(trimmed)
	}
	if len(hashPrefix) > 8 {
		hashPrefix = hashPrefix[:8]
	}
	switch {
	case len(trimmed) <= 8:
		return "***#" + hashPrefix
	case len(trimmed) <= 16:
		return trimmed[:2] + "***" + trimmed[len(trimmed)-2:] + "#" + hashPrefix
	default:
		return trimmed[:4] + "***" + trimmed[len(trimmed)-4:] + "#" + hashPrefix
	}
}

func isUsageDisplayLabel(value string) bool {
	return strings.Contains(value, "***") && strings.Contains(value, "#")
}

func looksSensitiveUsageIdentifier(value string) bool {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" || strings.EqualFold(trimmed, "unknown") {
		return false
	}
	if isUsageDisplayLabel(trimmed) {
		return false
	}
	if strings.ContainsAny(trimmed, " /") {
		return false
	}
	switch strings.ToLower(trimmed) {
	case "openai", "gemini", "gemini-cli", "claude", "codex", "vertex", "antigravity", "kimi":
		return false
	}
	return true
}

func snapshotFromUsageRecords(records []UsageRecord) StatisticsSnapshot {
	stats := NewRequestStatistics()
	stats.mu.Lock()
	for _, record := range records {
		apiName := record.APIKey
		if apiName == "" {
			apiName = "unknown"
		}
		modelName := record.Model
		if modelName == "" {
			modelName = "unknown"
		}
		if stats.apis == nil {
			stats.apis = make(map[string]*apiStats)
		}
		apiStatsValue, ok := stats.apis[apiName]
		if !ok || apiStatsValue == nil {
			apiStatsValue = &apiStats{Models: make(map[string]*modelStats)}
			stats.apis[apiName] = apiStatsValue
		} else if apiStatsValue.Models == nil {
			apiStatsValue.Models = make(map[string]*modelStats)
		}
		requestedAt := record.RequestedAt
		if requestedAt.IsZero() {
			requestedAt = time.Now()
		}
		stats.recordImported(apiName, modelName, apiStatsValue, RequestDetail{
			Timestamp: requestedAt,
			LatencyMs: record.LatencyMs,
			Source:    record.Source,
			AuthIndex: record.AuthIndex,
			Tokens:    normaliseTokenStats(record.Tokens),
			Failed:    record.Failed,
		})
	}
	stats.mu.Unlock()
	return stats.Snapshot()
}

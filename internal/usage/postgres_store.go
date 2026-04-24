package usage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

const defaultUsageTable = "usage_records"
const defaultSnapshotDetailLimit = 10000

// PostgresStoreConfig captures the database location for durable usage records.
type PostgresStoreConfig struct {
	DSN    string
	Schema string
	Table  string
}

// PostgresStatisticsStore persists request-level usage records in PostgreSQL.
type PostgresStatisticsStore struct {
	db     *sql.DB
	schema string
	table  string
}

// NewPostgresStatisticsStore opens a PostgreSQL-backed usage statistics store.
func NewPostgresStatisticsStore(ctx context.Context, cfg PostgresStoreConfig) (*PostgresStatisticsStore, error) {
	dsn := strings.TrimSpace(cfg.DSN)
	if dsn == "" {
		return nil, fmt.Errorf("postgres usage store: DSN is required")
	}
	table := strings.TrimSpace(cfg.Table)
	if table == "" {
		table = defaultUsageTable
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres usage store: open database: %w", err)
	}
	if err = db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("postgres usage store: ping database: %w", err)
	}
	return &PostgresStatisticsStore{
		db:     db,
		schema: strings.TrimSpace(cfg.Schema),
		table:  table,
	}, nil
}

// Close releases the database connection owned by the usage store.
func (s *PostgresStatisticsStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// EnsureSchema creates the usage table and indexes if they do not already exist.
func (s *PostgresStatisticsStore) EnsureSchema(ctx context.Context) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("postgres usage store: not initialized")
	}
	if s.schema != "" {
		query := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", quoteUsageIdentifier(s.schema))
		if _, err := s.db.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("postgres usage store: create schema: %w", err)
		}
	}
	table := s.fullTableName()
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id BIGSERIAL PRIMARY KEY,
			record_key TEXT NOT NULL,
			api_key TEXT NOT NULL DEFAULT '',
			api_key_hash TEXT NOT NULL DEFAULT '',
			provider TEXT NOT NULL DEFAULT '',
			model TEXT NOT NULL DEFAULT 'unknown',
			source TEXT NOT NULL DEFAULT '',
			auth_id TEXT NOT NULL DEFAULT '',
			auth_index TEXT NOT NULL DEFAULT '',
			requested_at TIMESTAMPTZ NOT NULL,
			latency_ms BIGINT NOT NULL DEFAULT 0,
			failed BOOLEAN NOT NULL DEFAULT FALSE,
			input_tokens BIGINT NOT NULL DEFAULT 0,
			output_tokens BIGINT NOT NULL DEFAULT 0,
			reasoning_tokens BIGINT NOT NULL DEFAULT 0,
			cached_tokens BIGINT NOT NULL DEFAULT 0,
			total_tokens BIGINT NOT NULL DEFAULT 0,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`, table)); err != nil {
		return fmt.Errorf("postgres usage store: create usage table: %w", err)
	}
	migrations := []string{
		"ADD COLUMN IF NOT EXISTS record_key TEXT",
		"ADD COLUMN IF NOT EXISTS api_key TEXT NOT NULL DEFAULT ''",
		"ADD COLUMN IF NOT EXISTS api_key_hash TEXT NOT NULL DEFAULT ''",
		"ADD COLUMN IF NOT EXISTS provider TEXT NOT NULL DEFAULT ''",
		"ADD COLUMN IF NOT EXISTS model TEXT NOT NULL DEFAULT 'unknown'",
		"ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT ''",
		"ADD COLUMN IF NOT EXISTS auth_id TEXT NOT NULL DEFAULT ''",
		"ADD COLUMN IF NOT EXISTS auth_index TEXT NOT NULL DEFAULT ''",
		"ADD COLUMN IF NOT EXISTS requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()",
		"ADD COLUMN IF NOT EXISTS latency_ms BIGINT NOT NULL DEFAULT 0",
		"ADD COLUMN IF NOT EXISTS failed BOOLEAN NOT NULL DEFAULT FALSE",
		"ADD COLUMN IF NOT EXISTS input_tokens BIGINT NOT NULL DEFAULT 0",
		"ADD COLUMN IF NOT EXISTS output_tokens BIGINT NOT NULL DEFAULT 0",
		"ADD COLUMN IF NOT EXISTS reasoning_tokens BIGINT NOT NULL DEFAULT 0",
		"ADD COLUMN IF NOT EXISTS cached_tokens BIGINT NOT NULL DEFAULT 0",
		"ADD COLUMN IF NOT EXISTS total_tokens BIGINT NOT NULL DEFAULT 0",
		"ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()",
	}
	for _, migration := range migrations {
		if _, err := s.db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s %s", table, migration)); err != nil {
			return fmt.Errorf("postgres usage store: migrate usage table: %w", err)
		}
	}
	indexes := []string{
		fmt.Sprintf("CREATE UNIQUE INDEX IF NOT EXISTS %s ON %s (record_key)", quoteUsageIdentifier(s.indexName("record_key")), table),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (requested_at)", quoteUsageIdentifier(s.indexName("requested_at")), table),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (model, requested_at)", quoteUsageIdentifier(s.indexName("model_time")), table),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (api_key_hash, requested_at)", quoteUsageIdentifier(s.indexName("api_time")), table),
	}
	for _, query := range indexes {
		if _, err := s.db.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("postgres usage store: create index: %w", err)
		}
	}
	return nil
}

// Record inserts one normalized usage record. Duplicate records are ignored.
func (s *PostgresStatisticsStore) Record(ctx context.Context, record UsageRecord) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("postgres usage store: not initialized")
	}
	record = normalizeStoredUsageRecord(record)
	_, err := s.db.ExecContext(ctx, s.insertSQL(), insertArgs(record)...)
	if err != nil {
		return fmt.Errorf("postgres usage store: insert usage record: %w", err)
	}
	return nil
}

// Snapshot rebuilds the management-page aggregate view from persisted records.
func (s *PostgresStatisticsStore) Snapshot(ctx context.Context) (StatisticsSnapshot, error) {
	if s == nil || s.db == nil {
		return StatisticsSnapshot{}, fmt.Errorf("postgres usage store: not initialized")
	}
	snapshot := StatisticsSnapshot{
		APIs:           make(map[string]APISnapshot),
		RequestsByDay:  make(map[string]int64),
		RequestsByHour: make(map[string]int64),
		TokensByDay:    make(map[string]int64),
		TokensByHour:   make(map[string]int64),
	}
	if err := s.loadSnapshotTotals(ctx, &snapshot); err != nil {
		return StatisticsSnapshot{}, err
	}
	if err := s.loadSnapshotModels(ctx, &snapshot); err != nil {
		return StatisticsSnapshot{}, err
	}
	if err := s.loadSnapshotTimeSeries(ctx, &snapshot); err != nil {
		return StatisticsSnapshot{}, err
	}
	if err := s.loadRecentSnapshotDetails(ctx, &snapshot, defaultSnapshotDetailLimit); err != nil {
		return StatisticsSnapshot{}, err
	}
	return snapshot, nil
}

// ImportSnapshot stores the request details from an exported snapshot.
func (s *PostgresStatisticsStore) ImportSnapshot(ctx context.Context, snapshot StatisticsSnapshot) (MergeResult, error) {
	result := MergeResult{}
	if s == nil || s.db == nil {
		return result, fmt.Errorf("postgres usage store: not initialized")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return result, fmt.Errorf("postgres usage store: begin import: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	stmt, err := tx.PrepareContext(ctx, s.insertSQL())
	if err != nil {
		return result, fmt.Errorf("postgres usage store: prepare import: %w", err)
	}
	defer stmt.Close()

	for apiName, apiSnapshot := range snapshot.APIs {
		apiName = strings.TrimSpace(apiName)
		if apiName == "" {
			continue
		}
		for modelName, modelSnapshot := range apiSnapshot.Models {
			modelName = strings.TrimSpace(modelName)
			if modelName == "" {
				modelName = "unknown"
			}
			for _, detail := range modelSnapshot.Details {
				record := usageRecordFromSnapshotDetail(apiName, modelName, detail)
				execResult, execErr := stmt.ExecContext(ctx, insertArgs(record)...)
				if execErr != nil {
					err = fmt.Errorf("postgres usage store: import usage record: %w", execErr)
					return result, err
				}
				if affected, affectedErr := execResult.RowsAffected(); affectedErr == nil && affected > 0 {
					result.Added++
				} else {
					result.Skipped++
				}
			}
		}
	}
	if err = tx.Commit(); err != nil {
		return result, fmt.Errorf("postgres usage store: commit import: %w", err)
	}
	return result, nil
}

func (s *PostgresStatisticsStore) insertSQL() string {
	return fmt.Sprintf(`
		INSERT INTO %s (
			record_key,
			api_key,
			api_key_hash,
			provider,
			model,
			source,
			auth_id,
			auth_index,
			requested_at,
			latency_ms,
			failed,
			input_tokens,
			output_tokens,
			reasoning_tokens,
			cached_tokens,
			total_tokens
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
		ON CONFLICT (record_key) DO NOTHING
	`, s.fullTableName())
}

func insertArgs(record UsageRecord) []any {
	return []any{
		record.RecordKey,
		record.APIKey,
		record.APIKeyHash,
		record.Provider,
		record.Model,
		record.Source,
		record.AuthID,
		record.AuthIndex,
		record.RequestedAt,
		record.LatencyMs,
		record.Failed,
		record.Tokens.InputTokens,
		record.Tokens.OutputTokens,
		record.Tokens.ReasoningTokens,
		record.Tokens.CachedTokens,
		record.Tokens.TotalTokens,
	}
}

func usageRecordFromSnapshotDetail(apiName, modelName string, detail RequestDetail) UsageRecord {
	if detail.Timestamp.IsZero() {
		detail.Timestamp = time.Now()
	}
	detail.Tokens = normaliseTokenStats(detail.Tokens)
	if detail.LatencyMs < 0 {
		detail.LatencyMs = 0
	}
	record := UsageRecord{
		APIKey:      usageDisplayLabel(apiName, looksSensitiveUsageIdentifier(apiName)),
		APIKeyHash:  hashUsageValue(apiName),
		Model:       modelName,
		Source:      detail.Source,
		AuthIndex:   detail.AuthIndex,
		RequestedAt: detail.Timestamp,
		LatencyMs:   detail.LatencyMs,
		Failed:      detail.Failed,
		Tokens:      detail.Tokens,
	}
	record.RecordKey = stableUsageRecordKey(apiName, modelName, detail)
	return normalizeStoredUsageRecord(record)
}

func normalizeStoredUsageRecord(record UsageRecord) UsageRecord {
	if record.APIKeyHash == "" {
		record.APIKeyHash = hashUsageValue(record.APIKey)
	}
	record.APIKey = safeStoredAPIKeyLabel(record.APIKey, record.APIKeyHash)
	record.Model = strings.TrimSpace(record.Model)
	if record.Model == "" {
		record.Model = "unknown"
	}
	record.Provider = strings.TrimSpace(record.Provider)
	record.Source = strings.TrimSpace(record.Source)
	record.AuthID = strings.TrimSpace(record.AuthID)
	record.AuthIndex = strings.TrimSpace(record.AuthIndex)
	if record.RequestedAt.IsZero() {
		record.RequestedAt = time.Now()
	}
	if record.LatencyMs < 0 {
		record.LatencyMs = 0
	}
	record.Tokens = normaliseTokenStats(record.Tokens)
	if record.RecordKey == "" {
		record.RecordKey = stableUsageRecordKey(record.APIKey, record.Model, RequestDetail{
			Timestamp: record.RequestedAt,
			LatencyMs: record.LatencyMs,
			Source:    record.Source,
			AuthIndex: record.AuthIndex,
			Tokens:    record.Tokens,
			Failed:    record.Failed,
		})
	}
	return record
}

func safeStoredAPIKeyLabel(apiKey, apiKeyHash string) string {
	apiKey = strings.TrimSpace(apiKey)
	if apiKey == "" {
		return "unknown"
	}
	return usageDisplayLabelWithHash(apiKey, apiKeyHash, looksSensitiveUsageIdentifier(apiKey))
}

func (s *PostgresStatisticsStore) loadSnapshotTotals(ctx context.Context, snapshot *StatisticsSnapshot) error {
	query := fmt.Sprintf(`
		SELECT
			COUNT(*),
			COALESCE(SUM(CASE WHEN failed THEN 0 ELSE 1 END), 0),
			COALESCE(SUM(CASE WHEN failed THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(total_tokens), 0)
		FROM %s
	`, s.fullTableName())
	err := s.db.QueryRowContext(ctx, query).Scan(
		&snapshot.TotalRequests,
		&snapshot.SuccessCount,
		&snapshot.FailureCount,
		&snapshot.TotalTokens,
	)
	if err != nil {
		return fmt.Errorf("postgres usage store: query usage totals: %w", err)
	}
	return nil
}

func (s *PostgresStatisticsStore) loadSnapshotModels(ctx context.Context, snapshot *StatisticsSnapshot) error {
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			api_key,
			api_key_hash,
			model,
			COUNT(*),
			COALESCE(SUM(total_tokens), 0)
		FROM %s
		GROUP BY api_key, api_key_hash, model
		ORDER BY api_key ASC, model ASC
	`, s.fullTableName()))
	if err != nil {
		return fmt.Errorf("postgres usage store: query usage model aggregates: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			apiName       string
			apiHash       string
			modelName     string
			totalRequests int64
			totalTokens   int64
		)
		if err = rows.Scan(&apiName, &apiHash, &modelName, &totalRequests, &totalTokens); err != nil {
			return fmt.Errorf("postgres usage store: scan usage model aggregate: %w", err)
		}
		apiName = safeStoredAPIKeyLabel(apiName, apiHash)
		apiSnapshot := snapshot.APIs[apiName]
		apiSnapshot.TotalRequests += totalRequests
		apiSnapshot.TotalTokens += totalTokens
		if apiSnapshot.Models == nil {
			apiSnapshot.Models = make(map[string]ModelSnapshot)
		}
		apiSnapshot.Models[modelName] = ModelSnapshot{
			TotalRequests: totalRequests,
			TotalTokens:   totalTokens,
			Details:       []RequestDetail{},
		}
		snapshot.APIs[apiName] = apiSnapshot
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("postgres usage store: iterate usage model aggregates: %w", err)
	}
	return nil
}

func (s *PostgresStatisticsStore) loadSnapshotTimeSeries(ctx context.Context, snapshot *StatisticsSnapshot) error {
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			TO_CHAR(requested_at AT TIME ZONE 'UTC', 'YYYY-MM-DD'),
			COUNT(*),
			COALESCE(SUM(total_tokens), 0)
		FROM %s
		GROUP BY 1
		ORDER BY 1 ASC
	`, s.fullTableName()))
	if err != nil {
		return fmt.Errorf("postgres usage store: query usage daily aggregates: %w", err)
	}
	for rows.Next() {
		var day string
		var requests, tokens int64
		if err = rows.Scan(&day, &requests, &tokens); err != nil {
			_ = rows.Close()
			return fmt.Errorf("postgres usage store: scan usage daily aggregate: %w", err)
		}
		snapshot.RequestsByDay[day] = requests
		snapshot.TokensByDay[day] = tokens
	}
	if err = rows.Close(); err != nil {
		return fmt.Errorf("postgres usage store: close usage daily aggregates: %w", err)
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("postgres usage store: iterate usage daily aggregates: %w", err)
	}

	rows, err = s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			EXTRACT(HOUR FROM requested_at AT TIME ZONE 'UTC')::INT,
			COUNT(*),
			COALESCE(SUM(total_tokens), 0)
		FROM %s
		GROUP BY 1
		ORDER BY 1 ASC
	`, s.fullTableName()))
	if err != nil {
		return fmt.Errorf("postgres usage store: query usage hourly aggregates: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var hour int
		var requests, tokens int64
		if err = rows.Scan(&hour, &requests, &tokens); err != nil {
			return fmt.Errorf("postgres usage store: scan usage hourly aggregate: %w", err)
		}
		hourKey := formatHour(hour)
		snapshot.RequestsByHour[hourKey] = requests
		snapshot.TokensByHour[hourKey] = tokens
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("postgres usage store: iterate usage hourly aggregates: %w", err)
	}
	return nil
}

func (s *PostgresStatisticsStore) loadRecentSnapshotDetails(ctx context.Context, snapshot *StatisticsSnapshot, limit int) error {
	if limit <= 0 {
		return nil
	}
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			api_key,
			api_key_hash,
			model,
			source,
			auth_index,
			requested_at,
			latency_ms,
			failed,
			input_tokens,
			output_tokens,
			reasoning_tokens,
			cached_tokens,
			total_tokens
		FROM %s
		ORDER BY requested_at DESC, id DESC
		LIMIT $1
	`, s.fullTableName()), limit)
	if err != nil {
		return fmt.Errorf("postgres usage store: query recent usage details: %w", err)
	}
	defer rows.Close()

	var details []UsageRecord
	for rows.Next() {
		var record UsageRecord
		if err = rows.Scan(
			&record.APIKey,
			&record.APIKeyHash,
			&record.Model,
			&record.Source,
			&record.AuthIndex,
			&record.RequestedAt,
			&record.LatencyMs,
			&record.Failed,
			&record.Tokens.InputTokens,
			&record.Tokens.OutputTokens,
			&record.Tokens.ReasoningTokens,
			&record.Tokens.CachedTokens,
			&record.Tokens.TotalTokens,
		); err != nil {
			return fmt.Errorf("postgres usage store: scan recent usage detail: %w", err)
		}
		details = append(details, normalizeStoredUsageRecord(record))
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("postgres usage store: iterate recent usage details: %w", err)
	}
	for i := len(details) - 1; i >= 0; i-- {
		record := details[i]
		apiSnapshot := snapshot.APIs[record.APIKey]
		if apiSnapshot.Models == nil {
			apiSnapshot.Models = make(map[string]ModelSnapshot)
		}
		modelSnapshot := apiSnapshot.Models[record.Model]
		modelSnapshot.Details = append(modelSnapshot.Details, RequestDetail{
			Timestamp: record.RequestedAt,
			LatencyMs: record.LatencyMs,
			Source:    record.Source,
			AuthIndex: record.AuthIndex,
			Tokens:    normaliseTokenStats(record.Tokens),
			Failed:    record.Failed,
		})
		apiSnapshot.Models[record.Model] = modelSnapshot
		snapshot.APIs[record.APIKey] = apiSnapshot
	}
	return nil
}

func (s *PostgresStatisticsStore) fullTableName() string {
	table := s.table
	if table == "" {
		table = defaultUsageTable
	}
	quotedTable := quoteUsageIdentifier(table)
	if s.schema == "" {
		return quotedTable
	}
	return quoteUsageIdentifier(s.schema) + "." + quotedTable
}

func (s *PostgresStatisticsStore) indexName(suffix string) string {
	table := s.table
	if table == "" {
		table = defaultUsageTable
	}
	name := strings.NewReplacer(".", "_", "-", "_", " ", "_").Replace(table)
	return fmt.Sprintf("idx_%s_%s", name, suffix)
}

func quoteUsageIdentifier(identifier string) string {
	parts := strings.Split(identifier, ".")
	for i, part := range parts {
		parts[i] = `"` + strings.ReplaceAll(part, `"`, `""`) + `"`
	}
	return strings.Join(parts, ".")
}

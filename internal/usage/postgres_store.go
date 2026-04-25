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
const defaultUsageSanitizeBatchSize = 500

const (
	usageAggregateModelsSuffix = "model_stats"
	usageAggregateDaysSuffix   = "day_stats"
	usageAggregateHoursSuffix  = "hour_stats"
	usageMetadataSuffix        = "metadata"
)

const usageSanitizedLabelsMetaKey = "sensitive_labels_sanitized_v1"

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

type usageSQLExecer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

type usageSanitizeUpdate struct {
	id     int64
	record UsageRecord
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
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (requested_at DESC, id DESC)", quoteUsageIdentifier(s.indexName("requested_id_desc")), table),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (model, requested_at)", quoteUsageIdentifier(s.indexName("model_time")), table),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (api_key_hash, requested_at)", quoteUsageIdentifier(s.indexName("api_time")), table),
	}
	for _, query := range indexes {
		if _, err := s.db.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("postgres usage store: create index: %w", err)
		}
	}
	if err := s.ensureMetadataSchema(ctx); err != nil {
		return err
	}
	labelsChanged, err := s.ensureStoredLabelsSanitized(ctx)
	if err != nil {
		return err
	}
	if err := s.ensureAggregateSchema(ctx); err != nil {
		return err
	}
	needsBackfill, err := s.aggregateTablesNeedBackfill(ctx)
	if err != nil {
		return err
	}
	if labelsChanged || needsBackfill {
		if err := s.rebuildAggregates(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (s *PostgresStatisticsStore) ensureAggregateSchema(ctx context.Context) error {
	modelTable := s.aggregateTableName(usageAggregateModelsSuffix)
	dayTable := s.aggregateTableName(usageAggregateDaysSuffix)
	hourTable := s.aggregateTableName(usageAggregateHoursSuffix)
	queries := []string{
		fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s (
				api_key_hash TEXT NOT NULL DEFAULT '',
				api_key TEXT NOT NULL DEFAULT '',
				provider TEXT NOT NULL DEFAULT '',
				model TEXT NOT NULL DEFAULT 'unknown',
				total_requests BIGINT NOT NULL DEFAULT 0,
				success_count BIGINT NOT NULL DEFAULT 0,
				failure_count BIGINT NOT NULL DEFAULT 0,
				total_tokens BIGINT NOT NULL DEFAULT 0,
				updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
				PRIMARY KEY (api_key_hash, api_key, model)
			)
		`, modelTable),
		fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s (
				day DATE PRIMARY KEY,
				total_requests BIGINT NOT NULL DEFAULT 0,
				success_count BIGINT NOT NULL DEFAULT 0,
				failure_count BIGINT NOT NULL DEFAULT 0,
				total_tokens BIGINT NOT NULL DEFAULT 0,
				updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
			)
		`, dayTable),
		fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s (
				hour SMALLINT PRIMARY KEY,
				total_requests BIGINT NOT NULL DEFAULT 0,
				success_count BIGINT NOT NULL DEFAULT 0,
				failure_count BIGINT NOT NULL DEFAULT 0,
				total_tokens BIGINT NOT NULL DEFAULT 0,
				updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
				CONSTRAINT %s CHECK (hour >= 0 AND hour <= 23)
			)
		`, hourTable, quoteUsageIdentifier(s.indexName("hour_range"))),
	}
	for _, query := range queries {
		if _, err := s.db.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("postgres usage store: create aggregate table: %w", err)
		}
	}
	return nil
}

func (s *PostgresStatisticsStore) ensureMetadataSchema(ctx context.Context) error {
	metadataTable := s.metadataTableName()
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL DEFAULT '',
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`, metadataTable)); err != nil {
		return fmt.Errorf("postgres usage store: create metadata table: %w", err)
	}
	return nil
}

func (s *PostgresStatisticsStore) ensureStoredLabelsSanitized(ctx context.Context) (bool, error) {
	done, err := s.metadataFlag(ctx, usageSanitizedLabelsMetaKey)
	if err != nil {
		return false, err
	}
	if done {
		return false, nil
	}
	changed, err := s.sanitizeStoredUsageLabels(ctx)
	if err != nil {
		return false, err
	}
	if err := s.setMetadataFlag(ctx, usageSanitizedLabelsMetaKey, "true"); err != nil {
		return changed, err
	}
	return changed, nil
}

// Record inserts one normalized usage record. Duplicate records are ignored.
func (s *PostgresStatisticsStore) Record(ctx context.Context, record UsageRecord) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("postgres usage store: not initialized")
	}
	record = normalizeStoredUsageRecord(record)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("postgres usage store: begin record: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	inserted, err := s.insertRecord(ctx, tx, record)
	if err != nil {
		return err
	}
	if inserted {
		if err = s.upsertAggregates(ctx, tx, record); err != nil {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("postgres usage store: commit usage record: %w", err)
	}
	return nil
}

func (s *PostgresStatisticsStore) insertRecord(ctx context.Context, execer usageSQLExecer, record UsageRecord) (bool, error) {
	result, err := execer.ExecContext(ctx, s.insertSQL(), insertArgs(record)...)
	if err != nil {
		return false, fmt.Errorf("postgres usage store: insert usage record: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("postgres usage store: inspect insert result: %w", err)
	}
	return affected > 0, nil
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

// CompleteSnapshot rebuilds a full-fidelity snapshot for backup/export paths.
// Unlike Snapshot, it includes all persisted request details.
func (s *PostgresStatisticsStore) CompleteSnapshot(ctx context.Context) (StatisticsSnapshot, error) {
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
	if err := s.loadRecentSnapshotDetails(ctx, &snapshot, 0); err != nil {
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
				inserted, execErr := s.insertRecord(ctx, tx, record)
				if execErr != nil {
					err = execErr
					return result, err
				}
				if !inserted {
					result.Skipped++
					continue
				}
				if err = s.upsertAggregates(ctx, tx, record); err != nil {
					return result, err
				}
				result.Added++
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

func (s *PostgresStatisticsStore) upsertAggregates(ctx context.Context, execer usageSQLExecer, record UsageRecord) error {
	successCount := int64(1)
	failureCount := int64(0)
	if record.Failed {
		successCount = 0
		failureCount = 1
	}
	totalTokens := record.Tokens.TotalTokens
	if totalTokens < 0 {
		totalTokens = 0
	}
	requestedAt := record.RequestedAt
	if requestedAt.IsZero() {
		requestedAt = time.Now()
	}
	day := requestedAt.UTC().Format("2006-01-02")
	hour := requestedAt.UTC().Hour()

	modelTable := s.aggregateTableName(usageAggregateModelsSuffix)
	dayTable := s.aggregateTableName(usageAggregateDaysSuffix)
	hourTable := s.aggregateTableName(usageAggregateHoursSuffix)

	if _, err := execer.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s AS agg (
			api_key_hash,
			api_key,
			provider,
			model,
			total_requests,
			success_count,
			failure_count,
			total_tokens,
			updated_at
		)
		VALUES ($1, $2, $3, $4, 1, $5, $6, $7, NOW())
		ON CONFLICT (api_key_hash, api_key, model) DO UPDATE SET
			provider = EXCLUDED.provider,
			total_requests = agg.total_requests + EXCLUDED.total_requests,
			success_count = agg.success_count + EXCLUDED.success_count,
			failure_count = agg.failure_count + EXCLUDED.failure_count,
			total_tokens = agg.total_tokens + EXCLUDED.total_tokens,
			updated_at = NOW()
	`, modelTable),
		record.APIKeyHash,
		record.APIKey,
		record.Provider,
		record.Model,
		successCount,
		failureCount,
		totalTokens,
	); err != nil {
		return fmt.Errorf("postgres usage store: update model aggregate: %w", err)
	}

	if _, err := execer.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s AS agg (
			day,
			total_requests,
			success_count,
			failure_count,
			total_tokens,
			updated_at
		)
		VALUES ($1::date, 1, $2, $3, $4, NOW())
		ON CONFLICT (day) DO UPDATE SET
			total_requests = agg.total_requests + EXCLUDED.total_requests,
			success_count = agg.success_count + EXCLUDED.success_count,
			failure_count = agg.failure_count + EXCLUDED.failure_count,
			total_tokens = agg.total_tokens + EXCLUDED.total_tokens,
			updated_at = NOW()
	`, dayTable),
		day,
		successCount,
		failureCount,
		totalTokens,
	); err != nil {
		return fmt.Errorf("postgres usage store: update daily aggregate: %w", err)
	}

	if _, err := execer.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s AS agg (
			hour,
			total_requests,
			success_count,
			failure_count,
			total_tokens,
			updated_at
		)
		VALUES ($1, 1, $2, $3, $4, NOW())
		ON CONFLICT (hour) DO UPDATE SET
			total_requests = agg.total_requests + EXCLUDED.total_requests,
			success_count = agg.success_count + EXCLUDED.success_count,
			failure_count = agg.failure_count + EXCLUDED.failure_count,
			total_tokens = agg.total_tokens + EXCLUDED.total_tokens,
			updated_at = NOW()
	`, hourTable),
		hour,
		successCount,
		failureCount,
		totalTokens,
	); err != nil {
		return fmt.Errorf("postgres usage store: update hourly aggregate: %w", err)
	}
	return nil
}

func (s *PostgresStatisticsStore) rebuildAggregates(ctx context.Context) (err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("postgres usage store: begin aggregate rebuild: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	tables := []string{
		s.aggregateTableName(usageAggregateModelsSuffix),
		s.aggregateTableName(usageAggregateDaysSuffix),
		s.aggregateTableName(usageAggregateHoursSuffix),
	}
	for _, table := range tables {
		if _, err = tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s", table)); err != nil {
			return fmt.Errorf("postgres usage store: clear aggregate table: %w", err)
		}
	}
	sourceTable := s.fullTableName()
	if _, err = tx.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (
			api_key_hash,
			api_key,
			provider,
			model,
			total_requests,
			success_count,
			failure_count,
			total_tokens,
			updated_at
		)
		SELECT
			api_key_hash,
			api_key,
			COALESCE(MAX(provider), ''),
			model,
			COUNT(*),
			COALESCE(SUM(CASE WHEN failed THEN 0 ELSE 1 END), 0),
			COALESCE(SUM(CASE WHEN failed THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(total_tokens), 0),
			NOW()
		FROM %s
		GROUP BY api_key_hash, api_key, model
	`, s.aggregateTableName(usageAggregateModelsSuffix), sourceTable)); err != nil {
		return fmt.Errorf("postgres usage store: rebuild model aggregates: %w", err)
	}
	if _, err = tx.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (
			day,
			total_requests,
			success_count,
			failure_count,
			total_tokens,
			updated_at
		)
		SELECT
			(requested_at AT TIME ZONE 'UTC')::date,
			COUNT(*),
			COALESCE(SUM(CASE WHEN failed THEN 0 ELSE 1 END), 0),
			COALESCE(SUM(CASE WHEN failed THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(total_tokens), 0),
			NOW()
		FROM %s
		GROUP BY 1
	`, s.aggregateTableName(usageAggregateDaysSuffix), sourceTable)); err != nil {
		return fmt.Errorf("postgres usage store: rebuild daily aggregates: %w", err)
	}
	if _, err = tx.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (
			hour,
			total_requests,
			success_count,
			failure_count,
			total_tokens,
			updated_at
		)
		SELECT
			EXTRACT(HOUR FROM requested_at AT TIME ZONE 'UTC')::INT,
			COUNT(*),
			COALESCE(SUM(CASE WHEN failed THEN 0 ELSE 1 END), 0),
			COALESCE(SUM(CASE WHEN failed THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(total_tokens), 0),
			NOW()
		FROM %s
		GROUP BY 1
	`, s.aggregateTableName(usageAggregateHoursSuffix), sourceTable)); err != nil {
		return fmt.Errorf("postgres usage store: rebuild hourly aggregates: %w", err)
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("postgres usage store: commit aggregate rebuild: %w", err)
	}
	return nil
}

func (s *PostgresStatisticsStore) aggregateTablesNeedBackfill(ctx context.Context) (bool, error) {
	sourceHasRows, err := s.tableHasRows(ctx, s.fullTableName())
	if err != nil {
		return false, err
	}
	if !sourceHasRows {
		return false, nil
	}
	for _, table := range []string{
		s.aggregateTableName(usageAggregateModelsSuffix),
		s.aggregateTableName(usageAggregateDaysSuffix),
		s.aggregateTableName(usageAggregateHoursSuffix),
	} {
		hasRows, err := s.tableHasRows(ctx, table)
		if err != nil {
			return false, err
		}
		if !hasRows {
			return true, nil
		}
	}
	return false, nil
}

func (s *PostgresStatisticsStore) tableHasRows(ctx context.Context, table string) (bool, error) {
	var exists bool
	if err := s.db.QueryRowContext(ctx, fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM %s LIMIT 1)", table)).Scan(&exists); err != nil {
		return false, fmt.Errorf("postgres usage store: check table rows: %w", err)
	}
	return exists, nil
}

func (s *PostgresStatisticsStore) metadataFlag(ctx context.Context, key string) (bool, error) {
	var value string
	err := s.db.QueryRowContext(ctx, fmt.Sprintf("SELECT value FROM %s WHERE key = $1", s.metadataTableName()), key).Scan(&value)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("postgres usage store: query metadata flag: %w", err)
	}
	return strings.EqualFold(strings.TrimSpace(value), "true"), nil
}

func (s *PostgresStatisticsStore) setMetadataFlag(ctx context.Context, key, value string) error {
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s AS metadata (key, value, updated_at)
		VALUES ($1, $2, NOW())
		ON CONFLICT (key) DO UPDATE SET
			value = EXCLUDED.value,
			updated_at = NOW()
	`, s.metadataTableName()), key, value); err != nil {
		return fmt.Errorf("postgres usage store: set metadata flag: %w", err)
	}
	return nil
}

func (s *PostgresStatisticsStore) sanitizeStoredUsageLabels(ctx context.Context) (bool, error) {
	var changed bool
	var lastID int64
	for {
		updates, nextID, err := s.loadUsageLabelSanitizeBatch(ctx, lastID)
		if err != nil {
			return changed, err
		}
		if nextID == lastID {
			return changed, nil
		}
		lastID = nextID
		if len(updates) == 0 {
			continue
		}
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return changed, fmt.Errorf("postgres usage store: begin label sanitize: %w", err)
		}
		if err = s.updateSanitizedUsageRecords(ctx, tx, updates); err != nil {
			_ = tx.Rollback()
			return changed, err
		}
		if err = tx.Commit(); err != nil {
			return changed, fmt.Errorf("postgres usage store: commit label sanitize: %w", err)
		}
		changed = true
	}
}

func (s *PostgresStatisticsStore) loadUsageLabelSanitizeBatch(ctx context.Context, afterID int64) ([]usageSanitizeUpdate, int64, error) {
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			id,
			COALESCE(record_key, ''),
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
		FROM %s
		WHERE id > $1
		ORDER BY id ASC
		LIMIT $2
	`, s.fullTableName()), afterID, defaultUsageSanitizeBatchSize)
	if err != nil {
		return nil, afterID, fmt.Errorf("postgres usage store: query label sanitize batch: %w", err)
	}
	defer rows.Close()

	var updates []usageSanitizeUpdate
	nextID := afterID
	for rows.Next() {
		var id int64
		var record UsageRecord
		if err = rows.Scan(
			&id,
			&record.RecordKey,
			&record.APIKey,
			&record.APIKeyHash,
			&record.Provider,
			&record.Model,
			&record.Source,
			&record.AuthID,
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
			return nil, afterID, fmt.Errorf("postgres usage store: scan label sanitize batch: %w", err)
		}
		nextID = id
		normalized := normalizeStoredUsageRecord(record)
		if normalized.RecordKey != record.RecordKey ||
			normalized.APIKey != record.APIKey ||
			normalized.APIKeyHash != record.APIKeyHash ||
			normalized.Source != record.Source {
			updates = append(updates, usageSanitizeUpdate{id: id, record: normalized})
		}
	}
	if err = rows.Err(); err != nil {
		return nil, afterID, fmt.Errorf("postgres usage store: iterate label sanitize batch: %w", err)
	}
	return updates, nextID, nil
}

func (s *PostgresStatisticsStore) updateSanitizedUsageRecords(ctx context.Context, execer usageSQLExecer, updates []usageSanitizeUpdate) error {
	if len(updates) == 0 {
		return nil
	}
	table := s.fullTableName()
	valueRows := make([]string, 0, len(updates))
	args := make([]any, 0, len(updates)*5)
	for i, update := range updates {
		base := i*5 + 1
		valueRows = append(valueRows, fmt.Sprintf("($%d::bigint, $%d::text, $%d::text, $%d::text, $%d::text)", base, base+1, base+2, base+3, base+4))
		args = append(args, update.id, update.record.APIKey, update.record.APIKeyHash, update.record.Source, update.record.RecordKey)
	}
	if _, err := execer.ExecContext(ctx, fmt.Sprintf(`
		WITH updates(id, api_key, api_key_hash, source, record_key) AS (
			VALUES %s
		),
		dedup AS (
			SELECT updates.*,
				NOT EXISTS (
					SELECT 1 FROM %s AS other
					WHERE other.record_key = updates.record_key
						AND other.id <> updates.id
				) AS can_update_record_key
			FROM updates
		)
		UPDATE %s AS target
		SET api_key = dedup.api_key,
			api_key_hash = dedup.api_key_hash,
			source = dedup.source,
			record_key = CASE
				WHEN dedup.can_update_record_key THEN dedup.record_key
				ELSE target.record_key
			END
		FROM dedup
		WHERE target.id = dedup.id
	`, strings.Join(valueRows, ","), table, table), args...); err != nil {
		return fmt.Errorf("postgres usage store: sanitize usage labels: %w", err)
	}
	return nil
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
	record.Source = safeStoredUsageSourceLabel(record.Source)
	record.AuthID = strings.TrimSpace(record.AuthID)
	record.AuthIndex = strings.TrimSpace(record.AuthIndex)
	if record.RequestedAt.IsZero() {
		record.RequestedAt = time.Now()
	}
	if record.LatencyMs < 0 {
		record.LatencyMs = 0
	}
	record.Tokens = normaliseTokenStats(record.Tokens)
	record.RecordKey = storedUsageRecordKey(record)
	return record
}

func storedUsageRecordKey(record UsageRecord) string {
	return stableUsageRecordKey(record.APIKey, record.Model, RequestDetail{
		Timestamp: record.RequestedAt,
		LatencyMs: record.LatencyMs,
		Source:    record.Source,
		AuthIndex: record.AuthIndex,
		Tokens:    record.Tokens,
		Failed:    record.Failed,
	})
}

func safeStoredAPIKeyLabel(apiKey, apiKeyHash string) string {
	apiKey = strings.TrimSpace(apiKey)
	if apiKey == "" {
		return "unknown"
	}
	return usageDisplayLabelWithHash(apiKey, apiKeyHash, looksSensitiveUsageIdentifier(apiKey))
}

func safeStoredUsageSourceLabel(source string) string {
	source = strings.TrimSpace(source)
	if source == "" {
		return ""
	}
	return usageDisplayLabelWithHash(source, hashUsageValue(source), looksSensitiveUsageIdentifier(source))
}

func (s *PostgresStatisticsStore) loadSnapshotTotals(ctx context.Context, snapshot *StatisticsSnapshot) error {
	query := fmt.Sprintf(`
		SELECT
			COALESCE(SUM(total_requests), 0),
			COALESCE(SUM(success_count), 0),
			COALESCE(SUM(failure_count), 0),
			COALESCE(SUM(total_tokens), 0)
		FROM %s
	`, s.aggregateTableName(usageAggregateDaysSuffix))
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
			total_requests,
			total_tokens
		FROM %s
		ORDER BY api_key ASC, model ASC
	`, s.aggregateTableName(usageAggregateModelsSuffix)))
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
			TO_CHAR(day, 'YYYY-MM-DD'),
			total_requests,
			total_tokens
		FROM %s
		ORDER BY day ASC
	`, s.aggregateTableName(usageAggregateDaysSuffix)))
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
			hour,
			total_requests,
			total_tokens
		FROM %s
		ORDER BY hour ASC
	`, s.aggregateTableName(usageAggregateHoursSuffix)))
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
	query := fmt.Sprintf(`
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
	`, s.fullTableName())
	var (
		rows *sql.Rows
		err  error
	)
	if limit > 0 {
		rows, err = s.db.QueryContext(ctx, query+" LIMIT $1", limit)
	} else {
		rows, err = s.db.QueryContext(ctx, query)
	}
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

func (s *PostgresStatisticsStore) aggregateTableName(suffix string) string {
	table := s.table
	if table == "" {
		table = defaultUsageTable
	}
	table = table + "_" + strings.TrimSpace(suffix)
	quotedTable := quoteUsageIdentifier(table)
	if s.schema == "" {
		return quotedTable
	}
	return quoteUsageIdentifier(s.schema) + "." + quotedTable
}

func (s *PostgresStatisticsStore) metadataTableName() string {
	return s.aggregateTableName(usageMetadataSuffix)
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

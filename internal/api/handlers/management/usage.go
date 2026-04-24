package management

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/usage"
	log "github.com/sirupsen/logrus"
)

type usageExportPayload struct {
	Version    int                      `json:"version"`
	ExportedAt time.Time                `json:"exported_at"`
	Usage      usage.StatisticsSnapshot `json:"usage"`
}

type usageImportPayload struct {
	Version int                      `json:"version"`
	Usage   usage.StatisticsSnapshot `json:"usage"`
}

// GetUsageStatistics returns the request statistics snapshot.
func (h *Handler) GetUsageStatistics(c *gin.Context) {
	snapshot := h.usageSnapshot(c)
	c.JSON(http.StatusOK, gin.H{
		"usage":           snapshot,
		"failed_requests": snapshot.FailureCount,
	})
}

// ExportUsageStatistics returns a complete usage snapshot for backup/migration.
func (h *Handler) ExportUsageStatistics(c *gin.Context) {
	snapshot := h.usageSnapshot(c)
	c.JSON(http.StatusOK, usageExportPayload{
		Version:    1,
		ExportedAt: time.Now().UTC(),
		Usage:      snapshot,
	})
}

// ImportUsageStatistics merges a previously exported usage snapshot into memory.
func (h *Handler) ImportUsageStatistics(c *gin.Context) {
	if h == nil || h.usageStats == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "usage statistics unavailable"})
		return
	}

	data, err := c.GetRawData()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to read request body"})
		return
	}

	var payload usageImportPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json"})
		return
	}
	if payload.Version != 0 && payload.Version != 1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "unsupported version"})
		return
	}

	result := h.importUsageSnapshot(c, payload.Usage)
	snapshot := h.usageSnapshot(c)
	c.JSON(http.StatusOK, gin.H{
		"added":           result.Added,
		"skipped":         result.Skipped,
		"total_requests":  snapshot.TotalRequests,
		"failed_requests": snapshot.FailureCount,
	})
}

func (h *Handler) usageSnapshot(c *gin.Context) usage.StatisticsSnapshot {
	var snapshot usage.StatisticsSnapshot
	if h != nil && h.usageStore != nil && c != nil && c.Request != nil {
		persistentSnapshot, err := h.usageStore.Snapshot(c.Request.Context())
		if err == nil {
			return persistentSnapshot
		}
		log.WithError(err).Warn("failed to load persisted usage statistics; falling back to memory")
	}
	if h != nil && h.usageStats != nil {
		snapshot = h.usageStats.Snapshot()
	}
	return snapshot
}

func (h *Handler) importUsageSnapshot(c *gin.Context, snapshot usage.StatisticsSnapshot) usage.MergeResult {
	var result usage.MergeResult
	if h == nil {
		return result
	}
	persistentImported := false
	if h.usageStore != nil && c != nil && c.Request != nil {
		persistentResult, err := h.usageStore.ImportSnapshot(c.Request.Context(), snapshot)
		if err == nil {
			result = persistentResult
			persistentImported = true
		} else {
			log.WithError(err).Warn("failed to import persisted usage statistics; importing into memory only")
		}
	}
	if h.usageStats != nil {
		memoryResult := h.usageStats.MergeSnapshot(snapshot)
		if !persistentImported {
			result = memoryResult
		}
	}
	return result
}

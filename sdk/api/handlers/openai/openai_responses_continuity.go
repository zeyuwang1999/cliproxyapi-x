package openai

import (
	"bytes"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const responsesContinuationCacheTTL = time.Hour

var defaultResponsesContinuationCache = newResponsesContinuationCache(0)

type responsesRequestContinuity struct {
	SessionKey   string
	PinnedAuthID string
}

type responsesContinuationEntry struct {
	SessionKey string
	AuthID     string
	LastSeen   time.Time
}

type responsesContinuationCache struct {
	mu         sync.Mutex
	ttl        time.Duration
	byResponse map[string]responsesContinuationEntry
	bySession  map[string]responsesContinuationEntry
}

func newResponsesContinuationCache(ttl time.Duration) *responsesContinuationCache {
	if ttl <= 0 {
		ttl = responsesContinuationCacheTTL
	}
	return &responsesContinuationCache{
		ttl:        ttl,
		byResponse: make(map[string]responsesContinuationEntry),
		bySession:  make(map[string]responsesContinuationEntry),
	}
}

func (c *responsesContinuationCache) cleanupLocked(now time.Time) {
	if c == nil || c.ttl <= 0 {
		return
	}
	cutoff := now.Add(-c.ttl)
	for responseID, entry := range c.byResponse {
		if entry.LastSeen.Before(cutoff) {
			delete(c.byResponse, responseID)
		}
	}
	for sessionKey, entry := range c.bySession {
		if entry.LastSeen.Before(cutoff) {
			delete(c.bySession, sessionKey)
		}
	}
}

func (c *responsesContinuationCache) resolve(previousResponseID, promptCacheKey string) responsesContinuationEntry {
	previousResponseID = strings.TrimSpace(previousResponseID)
	promptCacheKey = strings.TrimSpace(promptCacheKey)
	if c == nil {
		return responsesContinuationEntry{SessionKey: promptCacheKey}
	}

	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cleanupLocked(now)

	var resolved responsesContinuationEntry
	if previousResponseID != "" {
		if entry, ok := c.byResponse[previousResponseID]; ok {
			entry.LastSeen = now
			c.byResponse[previousResponseID] = entry
			resolved = entry
		}
	}
	if promptCacheKey != "" {
		if resolved.SessionKey == "" {
			resolved.SessionKey = promptCacheKey
		}
		if entry, ok := c.bySession[promptCacheKey]; ok {
			entry.LastSeen = now
			c.bySession[promptCacheKey] = entry
			if resolved.AuthID == "" {
				resolved.AuthID = entry.AuthID
			}
			if resolved.SessionKey == "" {
				resolved.SessionKey = entry.SessionKey
			}
		}
	}
	return resolved
}

func (c *responsesContinuationCache) bindSessionAuth(sessionKey, authID string) {
	sessionKey = strings.TrimSpace(sessionKey)
	authID = strings.TrimSpace(authID)
	if c == nil || sessionKey == "" {
		return
	}

	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cleanupLocked(now)
	c.bySession[sessionKey] = responsesContinuationEntry{
		SessionKey: sessionKey,
		AuthID:     authID,
		LastSeen:   now,
	}
}

func (c *responsesContinuationCache) unbindSessionAuth(sessionKey, authID string) {
	sessionKey = strings.TrimSpace(sessionKey)
	authID = strings.TrimSpace(authID)
	if c == nil || sessionKey == "" || authID == "" {
		return
	}

	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cleanupLocked(now)
	entry, ok := c.bySession[sessionKey]
	if ok && strings.TrimSpace(entry.AuthID) == authID {
		delete(c.bySession, sessionKey)
	}
	for responseID, entry := range c.byResponse {
		if strings.TrimSpace(entry.AuthID) == authID && strings.TrimSpace(entry.SessionKey) == sessionKey {
			delete(c.byResponse, responseID)
		}
	}
}

func (c *responsesContinuationCache) bindResponse(responseID, sessionKey, authID string) {
	responseID = strings.TrimSpace(responseID)
	sessionKey = strings.TrimSpace(sessionKey)
	authID = strings.TrimSpace(authID)
	if c == nil || responseID == "" || sessionKey == "" {
		return
	}

	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cleanupLocked(now)

	entry := responsesContinuationEntry{
		SessionKey: sessionKey,
		AuthID:     authID,
		LastSeen:   now,
	}
	c.byResponse[responseID] = entry
	c.bySession[sessionKey] = entry
}

func prepareResponsesContinuityRequest(rawJSON []byte) ([]byte, responsesRequestContinuity) {
	return prepareResponsesContinuityRequestWithCache(defaultResponsesContinuationCache, rawJSON)
}

func prepareResponsesContinuityRequestWithCache(cache *responsesContinuationCache, rawJSON []byte) ([]byte, responsesRequestContinuity) {
	prevResponseID := strings.TrimSpace(gjson.GetBytes(rawJSON, "previous_response_id").String())
	promptCacheKey := strings.TrimSpace(gjson.GetBytes(rawJSON, "prompt_cache_key").String())
	resolved := cache.resolve(prevResponseID, promptCacheKey)

	sessionKey := promptCacheKey
	if sessionKey == "" {
		sessionKey = resolved.SessionKey
	}
	if sessionKey == "" {
		sessionKey = uuid.NewString()
	}

	state := responsesRequestContinuity{
		SessionKey:   sessionKey,
		PinnedAuthID: strings.TrimSpace(resolved.AuthID),
	}

	updated := bytes.Clone(rawJSON)
	if promptCacheKey == "" {
		if next, err := sjson.SetBytes(updated, "prompt_cache_key", sessionKey); err == nil {
			updated = next
		}
	}
	// HTTP Codex upstream does not accept previous_response_id. Build a full
	// transcript when tool outputs continue a previous response.
	updated = repairResponsesToolCallsForTranscript(sessionKey, updated)
	return updated, state
}

func recordResponsesContinuityFromPayload(sessionKey, authID string, payload []byte) {
	recordResponsesContinuityFromPayloadWithCaches(defaultResponsesContinuationCache, defaultWebsocketToolCallCache, sessionKey, authID, payload)
}

func recordResponsesContinuityFromPayloadWithCaches(cache *responsesContinuationCache, callCache *websocketToolOutputCache, sessionKey, authID string, payload []byte) {
	sessionKey = strings.TrimSpace(sessionKey)
	authID = strings.TrimSpace(authID)
	if sessionKey == "" || len(payload) == 0 {
		return
	}

	if cache != nil && authID != "" {
		cache.bindSessionAuth(sessionKey, authID)
	}

	eventType := strings.TrimSpace(gjson.GetBytes(payload, "type").String())
	switch eventType {
	case "response.completed", "response.output_item.added", "response.output_item.done":
		recordResponsesWebsocketToolCallsFromPayloadWithCache(callCache, sessionKey, payload)
		if eventType == "response.completed" && cache != nil {
			cache.bindResponse(gjson.GetBytes(payload, "response.id").String(), sessionKey, authID)
		}
		return
	}

	rootID := strings.TrimSpace(gjson.GetBytes(payload, "id").String())
	output := gjson.GetBytes(payload, "output")
	if rootID == "" && (!output.Exists() || !output.IsArray()) {
		return
	}
	if output.IsArray() && callCache != nil {
		for _, item := range output.Array() {
			if !isResponsesToolCallType(item.Get("type").String()) {
				continue
			}
			callID := strings.TrimSpace(item.Get("call_id").String())
			if callID == "" {
				continue
			}
			callCache.record(sessionKey, callID, []byte(item.Raw))
		}
	}
	if cache != nil {
		cache.bindResponse(rootID, sessionKey, authID)
	}
}

func recordResponsesContinuityFromSSEFrame(sessionKey, authID string, frame []byte) {
	for _, line := range bytes.Split(frame, []byte("\n")) {
		trimmed := bytes.TrimSpace(bytes.TrimRight(line, "\r"))
		if len(trimmed) == 0 || !bytes.HasPrefix(trimmed, []byte("data:")) {
			continue
		}
		data := bytes.TrimSpace(trimmed[len("data:"):])
		if len(data) == 0 || bytes.Equal(data, []byte("[DONE]")) {
			continue
		}
		recordResponsesContinuityFromPayload(sessionKey, authID, data)
	}
}

func responsesPinnedAuthIDFromRequest(r *http.Request, rawJSON []byte) string {
	if r == nil {
		return ""
	}
	promptCacheKey := strings.TrimSpace(gjson.GetBytes(rawJSON, "prompt_cache_key").String())
	prevResponseID := strings.TrimSpace(gjson.GetBytes(rawJSON, "previous_response_id").String())
	return defaultResponsesContinuationCache.resolve(prevResponseID, promptCacheKey).AuthID
}

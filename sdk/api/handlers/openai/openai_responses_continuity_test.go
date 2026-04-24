package openai

import (
	"testing"
	"time"

	"github.com/tidwall/gjson"
)

func TestPrepareResponsesContinuityRequestUsesCachedSessionAndAuth(t *testing.T) {
	cache := newResponsesContinuationCache(time.Minute)
	cache.bindResponse("resp-1", "session-1", "auth-1")

	raw := []byte(`{"previous_response_id":"resp-1","input":[{"type":"function_call_output","call_id":"call-1","output":"ok"}]}`)
	updated, state := prepareResponsesContinuityRequestWithCache(cache, raw)

	if state.SessionKey != "session-1" {
		t.Fatalf("session key = %q, want session-1", state.SessionKey)
	}
	if state.PinnedAuthID != "auth-1" {
		t.Fatalf("pinned auth = %q, want auth-1", state.PinnedAuthID)
	}
	if got := gjson.GetBytes(updated, "prompt_cache_key").String(); got != "session-1" {
		t.Fatalf("prompt_cache_key = %q, want session-1", got)
	}
	if got := gjson.GetBytes(updated, "previous_response_id").String(); got != "resp-1" {
		t.Fatalf("previous_response_id = %q, want resp-1", got)
	}
}

func TestPrepareResponsesContinuityRequestGeneratesPromptCacheKey(t *testing.T) {
	cache := newResponsesContinuationCache(time.Minute)

	raw := []byte(`{"model":"gpt-5.4","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}]}`)
	updated, state := prepareResponsesContinuityRequestWithCache(cache, raw)

	if state.SessionKey == "" {
		t.Fatal("expected generated session key")
	}
	if got := gjson.GetBytes(updated, "prompt_cache_key").String(); got != state.SessionKey {
		t.Fatalf("prompt_cache_key = %q, want %q", got, state.SessionKey)
	}
}

func TestRecordResponsesContinuityFromPayloadStoresResponseAndToolCall(t *testing.T) {
	cache := newResponsesContinuationCache(time.Minute)
	callCache := newWebsocketToolOutputCache(time.Minute, 10)
	payload := []byte(`{"id":"resp-1","object":"response","output":[{"type":"function_call","id":"fc-1","call_id":"call-1","name":"tool","arguments":"{}"}]}`)

	recordResponsesContinuityFromPayloadWithCaches(cache, callCache, "session-1", "auth-1", payload)

	resolved := cache.resolve("resp-1", "")
	if resolved.SessionKey != "session-1" {
		t.Fatalf("resolved session key = %q, want session-1", resolved.SessionKey)
	}
	if resolved.AuthID != "auth-1" {
		t.Fatalf("resolved auth = %q, want auth-1", resolved.AuthID)
	}

	cached, ok := callCache.get("session-1", "call-1")
	if !ok {
		t.Fatal("expected cached tool call")
	}
	if gjson.GetBytes(cached, "type").String() != "function_call" {
		t.Fatalf("cached tool type = %q, want function_call", gjson.GetBytes(cached, "type").String())
	}
}

func TestUnbindSessionAuthClearsResponseContinuity(t *testing.T) {
	cache := newResponsesContinuationCache(time.Minute)
	cache.bindResponse("resp-1", "session-1", "auth-1")
	cache.bindResponse("resp-2", "session-2", "auth-1")
	cache.bindResponse("resp-3", "session-1", "auth-2")

	cache.unbindSessionAuth("session-1", "auth-1")

	if resolved := cache.resolve("resp-1", ""); resolved.AuthID != "" || resolved.SessionKey != "" {
		t.Fatalf("resp-1 continuity = %+v, want cleared", resolved)
	}
	if resolved := cache.resolve("", "session-1"); resolved.AuthID != "auth-2" {
		t.Fatalf("session-1 continuity = %+v, want auth-2", resolved)
	}
	if resolved := cache.resolve("resp-2", ""); resolved.AuthID != "auth-1" || resolved.SessionKey != "session-2" {
		t.Fatalf("resp-2 continuity = %+v, want preserved auth-1/session-2", resolved)
	}
}

package executor

import (
	"bytes"
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/cache"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func testGeminiSignaturePayload() string {
	payload := append([]byte{0x0A}, bytes.Repeat([]byte{0x56}, 48)...)
	return base64.StdEncoding.EncodeToString(payload)
}

func testAntigravityAuth(baseURL string) *cliproxyauth.Auth {
	return &cliproxyauth.Auth{
		Attributes: map[string]string{
			"base_url": baseURL,
		},
		Metadata: map[string]any{
			"access_token": "token-123",
			"expired":      time.Now().Add(24 * time.Hour).Format(time.RFC3339),
		},
	}
}

func invalidClaudeThinkingPayload() []byte {
	return []byte(`{
		"model": "claude-sonnet-4-5-thinking",
		"messages": [
			{
				"role": "assistant",
				"content": [
					{"type": "thinking", "thinking": "bad", "signature": "` + testGeminiSignaturePayload() + `"},
					{"type": "text", "text": "hello"}
				]
			}
		]
	}`)
}

func TestAntigravityExecutor_StrictBypassStripsInvalidSignatureBeforeForwarding(t *testing.T) {
	previousCache := cache.SignatureCacheEnabled()
	previousStrict := cache.SignatureBypassStrictMode()
	cache.SetSignatureCacheEnabled(false)
	cache.SetSignatureBypassStrictMode(true)
	t.Cleanup(func() {
		cache.SetSignatureCacheEnabled(previousCache)
		cache.SetSignatureBypassStrictMode(previousStrict)
	})

	var hits atomic.Int32
	var bodiesMu sync.Mutex
	var bodies [][]byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if r.Body != nil {
			body, _ := io.ReadAll(r.Body)
			bodiesMu.Lock()
			bodies = append(bodies, append([]byte(nil), body...))
			bodiesMu.Unlock()
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"response":{"candidates":[{"content":{"parts":[{"text":"ok"}]}}]}}`))
	}))
	defer server.Close()

	executor := NewAntigravityExecutor(nil)
	auth := testAntigravityAuth(server.URL)
	payload := invalidClaudeThinkingPayload()
	opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("claude"), OriginalRequest: payload}
	req := cliproxyexecutor.Request{Model: "claude-sonnet-4-5-thinking", Payload: payload}

	sanitized, err := validateAntigravityRequestSignatures(opts.SourceFormat, payload)
	if err != nil {
		t.Fatalf("validateAntigravityRequestSignatures() error = %v", err)
	}
	if bytes.Contains(sanitized, []byte(`"type": "thinking"`)) {
		t.Fatalf("sanitized payload still contains thinking block: %s", sanitized)
	}
	if !bytes.Contains(sanitized, []byte(`"text": "hello"`)) {
		t.Fatalf("sanitized payload dropped non-thinking content: %s", sanitized)
	}

	tests := []struct {
		name   string
		invoke func() error
	}{
		{
			name: "execute",
			invoke: func() error {
				_, err := executor.Execute(context.Background(), auth, req, opts)
				return err
			},
		},
		{
			name: "stream",
			invoke: func() error {
				_, err := executor.ExecuteStream(context.Background(), auth, req, cliproxyexecutor.Options{SourceFormat: opts.SourceFormat, OriginalRequest: payload, Stream: true})
				return err
			},
		},
		{
			name: "count tokens",
			invoke: func() error {
				_, err := executor.CountTokens(context.Background(), auth, req, opts)
				return err
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			err := tt.invoke()
			if err != nil {
				t.Fatalf("expected invalid signature to be stripped before forwarding, got error: %v", err)
			}
		})
	}

	if got := hits.Load(); got != int32(len(tests)) {
		t.Fatalf("expected %d upstream hits after stripping invalid signatures, got %d", len(tests), got)
	}

	bodiesMu.Lock()
	defer bodiesMu.Unlock()
	if len(bodies) != len(tests) {
		t.Fatalf("captured %d upstream bodies, want %d", len(bodies), len(tests))
	}
	for index, body := range bodies {
		if bytes.Contains(body, []byte(`"type": "thinking"`)) {
			t.Fatalf("upstream body #%d still contains thinking block: %s", index, body)
		}
		if !bytes.Contains(body, []byte(`hello`)) {
			t.Fatalf("upstream body #%d dropped text content: %s", index, body)
		}
	}
}

func TestAntigravityExecutor_NonStrictBypassSkipsPrecheck(t *testing.T) {
	previousCache := cache.SignatureCacheEnabled()
	previousStrict := cache.SignatureBypassStrictMode()
	cache.SetSignatureCacheEnabled(false)
	cache.SetSignatureBypassStrictMode(false)
	t.Cleanup(func() {
		cache.SetSignatureCacheEnabled(previousCache)
		cache.SetSignatureBypassStrictMode(previousStrict)
	})

	payload := invalidClaudeThinkingPayload()
	from := sdktranslator.FromString("claude")

	_, err := validateAntigravityRequestSignatures(from, payload)
	if err != nil {
		t.Fatalf("non-strict bypass should skip precheck, got: %v", err)
	}
}

func TestAntigravityExecutor_CacheModeSkipsPrecheck(t *testing.T) {
	previous := cache.SignatureCacheEnabled()
	cache.SetSignatureCacheEnabled(true)
	t.Cleanup(func() {
		cache.SetSignatureCacheEnabled(previous)
	})

	payload := invalidClaudeThinkingPayload()
	from := sdktranslator.FromString("claude")

	_, err := validateAntigravityRequestSignatures(from, payload)
	if err != nil {
		t.Fatalf("cache mode should skip precheck, got: %v", err)
	}
}

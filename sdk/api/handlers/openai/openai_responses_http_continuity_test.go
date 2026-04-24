package openai

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/tidwall/gjson"
)

type httpResponsesContinuityCaptureExecutor struct {
	mu       sync.Mutex
	authIDs  []string
	payloads [][]byte
}

func (e *httpResponsesContinuityCaptureExecutor) Identifier() string { return "test-provider" }

func (e *httpResponsesContinuityCaptureExecutor) Execute(_ context.Context, auth *coreauth.Auth, req coreexecutor.Request, _ coreexecutor.Options) (coreexecutor.Response, error) {
	e.mu.Lock()
	callIndex := len(e.payloads)
	e.payloads = append(e.payloads, append([]byte(nil), req.Payload...))
	if auth != nil {
		e.authIDs = append(e.authIDs, auth.ID)
	} else {
		e.authIDs = append(e.authIDs, "")
	}
	e.mu.Unlock()

	switch callIndex {
	case 0:
		return coreexecutor.Response{
			Payload: []byte(`{"id":"resp-1","object":"response","output":[{"type":"function_call","id":"fc-1","call_id":"call-1","name":"tool","arguments":"{}"}]}`),
		}, nil
	case 1:
		return coreexecutor.Response{
			Payload: []byte(`{"id":"resp-2","object":"response","output":[{"type":"message","id":"msg-2","role":"assistant","content":[{"type":"output_text","text":"done"}]}]}`),
		}, nil
	default:
		return coreexecutor.Response{}, errors.New("unexpected execute call")
	}
}

func (e *httpResponsesContinuityCaptureExecutor) ExecuteStream(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	return nil, errors.New("not implemented")
}

func (e *httpResponsesContinuityCaptureExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (e *httpResponsesContinuityCaptureExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *httpResponsesContinuityCaptureExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func (e *httpResponsesContinuityCaptureExecutor) AuthIDs() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.authIDs...)
}

func (e *httpResponsesContinuityCaptureExecutor) Payloads() [][]byte {
	e.mu.Lock()
	defer e.mu.Unlock()

	payloads := make([][]byte, len(e.payloads))
	for i := range e.payloads {
		payloads[i] = append([]byte(nil), e.payloads[i]...)
	}
	return payloads
}

type httpResponsesContinuityFailoverExecutor struct {
	mu      sync.Mutex
	authIDs []string
}

func (e *httpResponsesContinuityFailoverExecutor) Identifier() string { return "test-provider" }

func (e *httpResponsesContinuityFailoverExecutor) Execute(_ context.Context, auth *coreauth.Auth, req coreexecutor.Request, _ coreexecutor.Options) (coreexecutor.Response, error) {
	e.mu.Lock()
	if auth != nil {
		e.authIDs = append(e.authIDs, auth.ID)
	} else {
		e.authIDs = append(e.authIDs, "")
	}
	e.mu.Unlock()

	if auth != nil && auth.ID == "auth-http-stuck-b" {
		return coreexecutor.Response{}, &coreauth.Error{
			Code:       "rate_limit_exceeded",
			Message:    "quota exhausted",
			HTTPStatus: http.StatusTooManyRequests,
		}
	}
	return coreexecutor.Response{
		Payload: []byte(`{"id":"resp-failover-ok","object":"response","output":[{"type":"message","id":"msg-ok","role":"assistant","content":[{"type":"output_text","text":"ok"}]}]}`),
	}, nil
}

func (e *httpResponsesContinuityFailoverExecutor) ExecuteStream(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	return nil, errors.New("not implemented")
}

func (e *httpResponsesContinuityFailoverExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (e *httpResponsesContinuityFailoverExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *httpResponsesContinuityFailoverExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func (e *httpResponsesContinuityFailoverExecutor) AuthIDs() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.authIDs...)
}

type httpResponsesContinuityPreviousResponseFailoverExecutor struct {
	mu      sync.Mutex
	authIDs []string
}

func (e *httpResponsesContinuityPreviousResponseFailoverExecutor) Identifier() string {
	return "test-provider"
}

func (e *httpResponsesContinuityPreviousResponseFailoverExecutor) Execute(_ context.Context, auth *coreauth.Auth, req coreexecutor.Request, _ coreexecutor.Options) (coreexecutor.Response, error) {
	e.mu.Lock()
	authID := ""
	if auth != nil {
		authID = auth.ID
	}
	e.authIDs = append(e.authIDs, authID)
	callIndex := len(e.authIDs)
	e.mu.Unlock()

	if authID == "auth-http-prev-b" && callIndex > 1 {
		return coreexecutor.Response{}, &coreauth.Error{
			Code:       "rate_limit_exceeded",
			Message:    "quota exhausted",
			HTTPStatus: http.StatusTooManyRequests,
		}
	}
	if authID == "auth-http-prev-a" {
		return coreexecutor.Response{
			Payload: []byte(`{"id":"resp-prev-after-failover","object":"response","output":[{"type":"message","id":"msg-ok","role":"assistant","content":[{"type":"output_text","text":"ok"}]}]}`),
		}, nil
	}
	return coreexecutor.Response{
		Payload: []byte(`{"id":"resp-prev-1","object":"response","output":[{"type":"function_call","id":"fc-1","call_id":"call-1","name":"tool","arguments":"{}"}]}`),
	}, nil
}

func (e *httpResponsesContinuityPreviousResponseFailoverExecutor) ExecuteStream(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	return nil, errors.New("not implemented")
}

func (e *httpResponsesContinuityPreviousResponseFailoverExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (e *httpResponsesContinuityPreviousResponseFailoverExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *httpResponsesContinuityPreviousResponseFailoverExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func (e *httpResponsesContinuityPreviousResponseFailoverExecutor) AuthIDs() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.authIDs...)
}

type httpResponsesContinuityStreamErrorExecutor struct {
	mu      sync.Mutex
	authIDs []string
}

func (e *httpResponsesContinuityStreamErrorExecutor) Identifier() string { return "test-provider" }

func (e *httpResponsesContinuityStreamErrorExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *httpResponsesContinuityStreamErrorExecutor) ExecuteStream(_ context.Context, auth *coreauth.Auth, _ coreexecutor.Request, _ coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	e.mu.Lock()
	if auth != nil {
		e.authIDs = append(e.authIDs, auth.ID)
	} else {
		e.authIDs = append(e.authIDs, "")
	}
	e.mu.Unlock()

	ch := make(chan coreexecutor.StreamChunk, 2)
	ch <- coreexecutor.StreamChunk{Payload: []byte(`data: {"type":"response.output_text.delta","delta":"working"}` + "\n\n")}
	ch <- coreexecutor.StreamChunk{Err: &coreauth.Error{
		Code:       "rate_limit_exceeded",
		Message:    "quota exhausted",
		HTTPStatus: http.StatusTooManyRequests,
	}}
	close(ch)
	return &coreexecutor.StreamResult{Chunks: ch}, nil
}

func (e *httpResponsesContinuityStreamErrorExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (e *httpResponsesContinuityStreamErrorExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *httpResponsesContinuityStreamErrorExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func TestOpenAIResponsesHTTPContinuationPinsAuthAndCarriesPromptCacheKey(t *testing.T) {
	gin.SetMode(gin.TestMode)

	oldContinuationCache := defaultResponsesContinuationCache
	oldOutputCache := defaultWebsocketToolOutputCache
	oldCallCache := defaultWebsocketToolCallCache
	defaultResponsesContinuationCache = newResponsesContinuationCache(time.Minute)
	defaultWebsocketToolOutputCache = newWebsocketToolOutputCache(time.Minute, 10)
	defaultWebsocketToolCallCache = newWebsocketToolOutputCache(time.Minute, 10)
	t.Cleanup(func() {
		defaultResponsesContinuationCache = oldContinuationCache
		defaultWebsocketToolOutputCache = oldOutputCache
		defaultWebsocketToolCallCache = oldCallCache
	})

	selector := &orderedWebsocketSelector{order: []string{"auth-http-b", "auth-http-a"}}
	executor := &httpResponsesContinuityCaptureExecutor{}
	manager := coreauth.NewManager(nil, selector, nil)
	manager.RegisterExecutor(executor)

	modelName := "test-http-responses-continuity"
	authA := &coreauth.Auth{ID: "auth-http-a", Provider: executor.Identifier(), Status: coreauth.StatusActive}
	authB := &coreauth.Auth{ID: "auth-http-b", Provider: executor.Identifier(), Status: coreauth.StatusActive}
	for _, auth := range []*coreauth.Auth{authA, authB} {
		if _, err := manager.Register(context.Background(), auth); err != nil {
			t.Fatalf("Register auth %s: %v", auth.ID, err)
		}
		registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: modelName}})
	}
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(authA.ID)
		registry.GetGlobalRegistry().UnregisterClient(authB.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.POST("/v1/responses", h.Responses)

	firstBody := `{"model":"` + modelName + `","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}]}`
	firstReq := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(firstBody))
	firstReq.Header.Set("Content-Type", "application/json")
	firstResp := httptest.NewRecorder()
	router.ServeHTTP(firstResp, firstReq)

	if firstResp.Code != http.StatusOK {
		t.Fatalf("first status = %d, want %d; body=%s", firstResp.Code, http.StatusOK, firstResp.Body.String())
	}
	if got := gjson.Get(firstResp.Body.String(), "id").String(); got != "resp-1" {
		t.Fatalf("first response id = %q, want resp-1", got)
	}

	secondBody := `{"model":"` + modelName + `","previous_response_id":"resp-1","input":[{"type":"function_call_output","call_id":"call-1","output":"ok"}]}`
	secondReq := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(secondBody))
	secondReq.Header.Set("Content-Type", "application/json")
	secondResp := httptest.NewRecorder()
	router.ServeHTTP(secondResp, secondReq)

	if secondResp.Code != http.StatusOK {
		t.Fatalf("second status = %d, want %d; body=%s", secondResp.Code, http.StatusOK, secondResp.Body.String())
	}
	if got := gjson.Get(secondResp.Body.String(), "id").String(); got != "resp-2" {
		t.Fatalf("second response id = %q, want resp-2", got)
	}

	authIDs := executor.AuthIDs()
	if len(authIDs) != 2 {
		t.Fatalf("auth call count = %d, want 2", len(authIDs))
	}
	if authIDs[0] != "auth-http-b" || authIDs[1] != "auth-http-b" {
		t.Fatalf("selected auth IDs = %v, want [auth-http-b auth-http-b]", authIDs)
	}

	payloads := executor.Payloads()
	if len(payloads) != 2 {
		t.Fatalf("payload count = %d, want 2", len(payloads))
	}

	firstPromptCacheKey := gjson.GetBytes(payloads[0], "prompt_cache_key").String()
	if firstPromptCacheKey == "" {
		t.Fatalf("first prompt_cache_key is empty: %s", payloads[0])
	}
	if got := gjson.GetBytes(payloads[1], "prompt_cache_key").String(); got != firstPromptCacheKey {
		t.Fatalf("second prompt_cache_key = %q, want %q", got, firstPromptCacheKey)
	}
	if got := gjson.GetBytes(payloads[1], "previous_response_id").String(); got != "resp-1" {
		t.Fatalf("second previous_response_id = %q, want resp-1", got)
	}

	secondInput := gjson.GetBytes(payloads[1], "input").Array()
	if len(secondInput) != 2 {
		t.Fatalf("second input len = %d, want 2: %s", len(secondInput), payloads[1])
	}
	if got := secondInput[0].Get("type").String(); got != "function_call" {
		t.Fatalf("second input[0].type = %q, want function_call", got)
	}
	if got := secondInput[0].Get("call_id").String(); got != "call-1" {
		t.Fatalf("second input[0].call_id = %q, want call-1", got)
	}
	if got := secondInput[1].Get("type").String(); got != "function_call_output" {
		t.Fatalf("second input[1].type = %q, want function_call_output", got)
	}
	if got := secondInput[1].Get("call_id").String(); got != "call-1" {
		t.Fatalf("second input[1].call_id = %q, want call-1", got)
	}

	if resolved := defaultResponsesContinuationCache.resolve("resp-1", ""); resolved.SessionKey != firstPromptCacheKey || resolved.AuthID != "auth-http-b" {
		t.Fatalf("resp-1 continuity = %+v, want session %q auth %q", resolved, firstPromptCacheKey, "auth-http-b")
	}
	if resolved := defaultResponsesContinuationCache.resolve("resp-2", ""); resolved.SessionKey != firstPromptCacheKey || resolved.AuthID != "auth-http-b" {
		t.Fatalf("resp-2 continuity = %+v, want session %q auth %q", resolved, firstPromptCacheKey, "auth-http-b")
	}
}

func TestOpenAIResponsesHTTPContinuationClearsPinnedAuthAfterFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)

	oldContinuationCache := defaultResponsesContinuationCache
	oldOutputCache := defaultWebsocketToolOutputCache
	oldCallCache := defaultWebsocketToolCallCache
	defaultResponsesContinuationCache = newResponsesContinuationCache(time.Minute)
	defaultWebsocketToolOutputCache = newWebsocketToolOutputCache(time.Minute, 10)
	defaultWebsocketToolCallCache = newWebsocketToolOutputCache(time.Minute, 10)
	t.Cleanup(func() {
		defaultResponsesContinuationCache = oldContinuationCache
		defaultWebsocketToolOutputCache = oldOutputCache
		defaultWebsocketToolCallCache = oldCallCache
	})

	selector := &orderedWebsocketSelector{order: []string{"auth-http-stuck-b", "auth-http-stuck-a"}}
	executor := &httpResponsesContinuityFailoverExecutor{}
	manager := coreauth.NewManager(nil, selector, nil)
	manager.RegisterExecutor(executor)

	modelName := "test-http-responses-continuity-failover"
	authA := &coreauth.Auth{ID: "auth-http-stuck-a", Provider: executor.Identifier(), Status: coreauth.StatusActive}
	authB := &coreauth.Auth{ID: "auth-http-stuck-b", Provider: executor.Identifier(), Status: coreauth.StatusActive}
	for _, auth := range []*coreauth.Auth{authA, authB} {
		if _, err := manager.Register(context.Background(), auth); err != nil {
			t.Fatalf("Register auth %s: %v", auth.ID, err)
		}
		registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: modelName}})
	}
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(authA.ID)
		registry.GetGlobalRegistry().UnregisterClient(authB.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.POST("/v1/responses", h.Responses)

	const stuckSession = "stuck-session"
	defaultResponsesContinuationCache.bindSessionAuth(stuckSession, authB.ID)
	body := `{"model":"` + modelName + `","prompt_cache_key":"` + stuckSession + `","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}]}`

	firstReq := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(body))
	firstReq.Header.Set("Content-Type", "application/json")
	firstResp := httptest.NewRecorder()
	router.ServeHTTP(firstResp, firstReq)

	if firstResp.Code != http.StatusTooManyRequests {
		t.Fatalf("first status = %d, want %d; body=%s", firstResp.Code, http.StatusTooManyRequests, firstResp.Body.String())
	}
	if resolved := defaultResponsesContinuationCache.resolve("", stuckSession); resolved.AuthID != "" {
		t.Fatalf("stuck session auth after failure = %q, want cleared", resolved.AuthID)
	}

	secondReq := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(body))
	secondReq.Header.Set("Content-Type", "application/json")
	secondResp := httptest.NewRecorder()
	router.ServeHTTP(secondResp, secondReq)

	if secondResp.Code != http.StatusOK {
		t.Fatalf("second status = %d, want %d; body=%s", secondResp.Code, http.StatusOK, secondResp.Body.String())
	}
	if got := gjson.Get(secondResp.Body.String(), "id").String(); got != "resp-failover-ok" {
		t.Fatalf("second response id = %q, want resp-failover-ok", got)
	}

	authIDs := executor.AuthIDs()
	if len(authIDs) == 0 || authIDs[len(authIDs)-1] != authA.ID {
		t.Fatalf("selected auth IDs = %v, want final auth %q", authIDs, authA.ID)
	}
	if resolved := defaultResponsesContinuationCache.resolve("", stuckSession); resolved.AuthID != authA.ID {
		t.Fatalf("stuck session auth after recovery = %q, want %q", resolved.AuthID, authA.ID)
	}
}

func TestOpenAIResponsesHTTPContinuationClearsPreviousResponseAfterFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)

	oldContinuationCache := defaultResponsesContinuationCache
	oldOutputCache := defaultWebsocketToolOutputCache
	oldCallCache := defaultWebsocketToolCallCache
	defaultResponsesContinuationCache = newResponsesContinuationCache(time.Minute)
	defaultWebsocketToolOutputCache = newWebsocketToolOutputCache(time.Minute, 10)
	defaultWebsocketToolCallCache = newWebsocketToolOutputCache(time.Minute, 10)
	t.Cleanup(func() {
		defaultResponsesContinuationCache = oldContinuationCache
		defaultWebsocketToolOutputCache = oldOutputCache
		defaultWebsocketToolCallCache = oldCallCache
	})

	selector := &orderedWebsocketSelector{order: []string{"auth-http-prev-b", "auth-http-prev-a"}}
	executor := &httpResponsesContinuityPreviousResponseFailoverExecutor{}
	manager := coreauth.NewManager(nil, selector, nil)
	manager.RegisterExecutor(executor)

	modelName := "test-http-responses-continuity-previous-response-failover"
	authA := &coreauth.Auth{ID: "auth-http-prev-a", Provider: executor.Identifier(), Status: coreauth.StatusActive}
	authB := &coreauth.Auth{ID: "auth-http-prev-b", Provider: executor.Identifier(), Status: coreauth.StatusActive}
	for _, auth := range []*coreauth.Auth{authA, authB} {
		if _, err := manager.Register(context.Background(), auth); err != nil {
			t.Fatalf("Register auth %s: %v", auth.ID, err)
		}
		registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: modelName}})
	}
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(authA.ID)
		registry.GetGlobalRegistry().UnregisterClient(authB.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.POST("/v1/responses", h.Responses)

	firstBody := `{"model":"` + modelName + `","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}]}`
	firstReq := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(firstBody))
	firstReq.Header.Set("Content-Type", "application/json")
	firstResp := httptest.NewRecorder()
	router.ServeHTTP(firstResp, firstReq)

	if firstResp.Code != http.StatusOK {
		t.Fatalf("first status = %d, want %d; body=%s", firstResp.Code, http.StatusOK, firstResp.Body.String())
	}
	if got := gjson.Get(firstResp.Body.String(), "id").String(); got != "resp-prev-1" {
		t.Fatalf("first response id = %q, want resp-prev-1", got)
	}

	followUpBody := `{"model":"` + modelName + `","previous_response_id":"resp-prev-1","input":[{"type":"function_call_output","call_id":"call-1","output":"ok"}]}`
	secondReq := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(followUpBody))
	secondReq.Header.Set("Content-Type", "application/json")
	secondResp := httptest.NewRecorder()
	router.ServeHTTP(secondResp, secondReq)

	if secondResp.Code != http.StatusTooManyRequests {
		t.Fatalf("second status = %d, want %d; body=%s", secondResp.Code, http.StatusTooManyRequests, secondResp.Body.String())
	}
	if resolved := defaultResponsesContinuationCache.resolve("resp-prev-1", ""); resolved.AuthID != "" {
		t.Fatalf("previous response auth after failure = %q, want cleared", resolved.AuthID)
	}

	thirdReq := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(followUpBody))
	thirdReq.Header.Set("Content-Type", "application/json")
	thirdResp := httptest.NewRecorder()
	router.ServeHTTP(thirdResp, thirdReq)

	if thirdResp.Code != http.StatusOK {
		t.Fatalf("third status = %d, want %d; body=%s", thirdResp.Code, http.StatusOK, thirdResp.Body.String())
	}
	if got := gjson.Get(thirdResp.Body.String(), "id").String(); got != "resp-prev-after-failover" {
		t.Fatalf("third response id = %q, want resp-prev-after-failover", got)
	}
	authIDs := executor.AuthIDs()
	if len(authIDs) != 3 || authIDs[0] != authB.ID || authIDs[1] != authB.ID || authIDs[2] != authA.ID {
		t.Fatalf("selected auth IDs = %v, want [%s %s %s]", authIDs, authB.ID, authB.ID, authA.ID)
	}
}

func TestOpenAIResponsesStreamingContinuationClearsSessionAfterTerminalError(t *testing.T) {
	gin.SetMode(gin.TestMode)

	oldContinuationCache := defaultResponsesContinuationCache
	oldOutputCache := defaultWebsocketToolOutputCache
	oldCallCache := defaultWebsocketToolCallCache
	defaultResponsesContinuationCache = newResponsesContinuationCache(time.Minute)
	defaultWebsocketToolOutputCache = newWebsocketToolOutputCache(time.Minute, 10)
	defaultWebsocketToolCallCache = newWebsocketToolOutputCache(time.Minute, 10)
	t.Cleanup(func() {
		defaultResponsesContinuationCache = oldContinuationCache
		defaultWebsocketToolOutputCache = oldOutputCache
		defaultWebsocketToolCallCache = oldCallCache
	})

	executor := &httpResponsesContinuityStreamErrorExecutor{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)

	modelName := "test-http-responses-continuity-stream-terminal-error"
	auth := &coreauth.Auth{ID: "auth-http-stream-b", Provider: executor.Identifier(), Status: coreauth.StatusActive}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register auth %s: %v", auth.ID, err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: modelName}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.POST("/v1/responses", h.Responses)

	const stuckSession = "stream-stuck-session"
	body := `{"model":"` + modelName + `","stream":true,"prompt_cache_key":"` + stuckSession + `","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("stream status = %d, want %d; body=%s", resp.Code, http.StatusOK, resp.Body.String())
	}
	if !strings.Contains(resp.Body.String(), "event: error") {
		t.Fatalf("stream body missing terminal error event: %s", resp.Body.String())
	}
	if resolved := defaultResponsesContinuationCache.resolve("", stuckSession); resolved.AuthID != "" {
		t.Fatalf("stream session auth after terminal error = %q, want cleared", resolved.AuthID)
	}
}

package openai

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/tidwall/gjson"
)

func TestCollectImagesFromResponsesWithRetriesRetriesDisconnectedStream(t *testing.T) {
	attempts := 0
	out, headers, errMsg := collectImagesFromResponsesWithRetries(context.Background(), 1, "b64_json", func() (<-chan []byte, http.Header, <-chan *interfaces.ErrorMessage) {
		attempts++
		if attempts == 1 {
			return imageDataChan(`data: {"type":"response.output_text.delta","delta":"working"}` + "\n\n"), http.Header{"X-Attempt": {"1"}}, imageErrChan()
		}
		return imageDataChan(`data: {"type":"response.completed","response":{"created_at":123,"output":[{"type":"image_generation_call","result":"aW1hZ2U=","output_format":"png","size":"1024x1024","quality":"medium"}],"tool_usage":{"image_gen":{"input_tokens":1,"output_tokens":2,"total_tokens":3}}}}` + "\n\n"), http.Header{"X-Attempt": {"2"}}, imageErrChan()
	})
	if errMsg != nil {
		t.Fatalf("unexpected error: %+v", errMsg)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
	if headers.Get("X-Attempt") != "2" {
		t.Fatalf("headers X-Attempt = %q, want 2", headers.Get("X-Attempt"))
	}
	if got := gjson.GetBytes(out, "data.0.b64_json").String(); got != "aW1hZ2U=" {
		t.Fatalf("b64_json = %q, want image result", got)
	}
	if got := gjson.GetBytes(out, "usage.total_tokens").Int(); got != 3 {
		t.Fatalf("usage.total_tokens = %d, want 3", got)
	}
}

func TestCollectImagesFromResponsesWithRetriesDoesNotRetryRequestError(t *testing.T) {
	attempts := 0
	_, _, errMsg := collectImagesFromResponsesWithRetries(context.Background(), 1, "b64_json", func() (<-chan []byte, http.Header, <-chan *interfaces.ErrorMessage) {
		attempts++
		return nil, nil, imageErrChan(&interfaces.ErrorMessage{
			StatusCode: http.StatusBadRequest,
			Error:      errors.New("invalid image request"),
		})
	})
	if errMsg == nil {
		t.Fatal("expected error")
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}

func TestCollectImagesFromResponsesRetriesThroughAuthManager(t *testing.T) {
	gin.SetMode(gin.TestMode)

	executor := &imageRetryStreamExecutor{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)

	auth1 := &coreauth.Auth{ID: "image-auth-1", Provider: "codex", Status: coreauth.StatusActive}
	auth2 := &coreauth.Auth{ID: "image-auth-2", Provider: "codex", Status: coreauth.StatusActive}
	for _, auth := range []*coreauth.Auth{auth1, auth2} {
		if _, err := manager.Register(context.Background(), auth); err != nil {
			t.Fatalf("manager.Register(%s): %v", auth.ID, err)
		}
		registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: defaultImagesMainModel}})
		t.Cleanup(func() {
			registry.GetGlobalRegistry().UnregisterClient(auth.ID)
		})
	}

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{
		Streaming: sdkconfig.StreamingConfig{BootstrapRetries: 1},
	}, manager)
	h := NewOpenAIAPIHandler(base)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/generations", nil)

	tool := []byte(`{"type":"image_generation","action":"generate","model":"gpt-image-2"}`)
	h.collectImagesFromResponses(c, buildImagesResponsesRequest("draw a test image", nil, tool), "b64_json")

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	if got := gjson.Get(w.Body.String(), "data.0.b64_json").String(); got != "aW1hZ2U=" {
		t.Fatalf("b64_json = %q, want image result", got)
	}
	if got := executor.AuthIDs(); len(got) != 2 || got[0] != "image-auth-1" || got[1] != "image-auth-2" {
		t.Fatalf("auth IDs = %v, want [image-auth-1 image-auth-2]", got)
	}
}

func imageDataChan(chunks ...string) <-chan []byte {
	ch := make(chan []byte, len(chunks))
	for _, chunk := range chunks {
		ch <- []byte(chunk)
	}
	close(ch)
	return ch
}

type imageRetryStreamExecutor struct {
	mu      sync.Mutex
	authIDs []string
}

func (e *imageRetryStreamExecutor) Identifier() string { return "codex" }

func (e *imageRetryStreamExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *imageRetryStreamExecutor) ExecuteStream(_ context.Context, auth *coreauth.Auth, _ coreexecutor.Request, _ coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	e.mu.Lock()
	e.authIDs = append(e.authIDs, auth.ID)
	attempt := len(e.authIDs)
	e.mu.Unlock()

	ch := make(chan coreexecutor.StreamChunk, 1)
	if attempt == 1 {
		ch <- coreexecutor.StreamChunk{Payload: []byte(`data: {"type":"response.output_text.delta","delta":"working"}` + "\n\n")}
		close(ch)
		return &coreexecutor.StreamResult{Headers: http.Header{"X-Attempt": {"1"}}, Chunks: ch}, nil
	}

	ch <- coreexecutor.StreamChunk{Payload: []byte(`data: {"type":"response.completed","response":{"created_at":123,"output":[{"type":"image_generation_call","result":"aW1hZ2U=","output_format":"png","size":"1024x1024","quality":"medium"}]}}` + "\n\n")}
	close(ch)
	return &coreexecutor.StreamResult{Headers: http.Header{"X-Attempt": {"2"}}, Chunks: ch}, nil
}

func (e *imageRetryStreamExecutor) Refresh(ctx context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (e *imageRetryStreamExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *imageRetryStreamExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func (e *imageRetryStreamExecutor) AuthIDs() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]string, len(e.authIDs))
	copy(out, e.authIDs)
	return out
}

func imageErrChan(errs ...*interfaces.ErrorMessage) <-chan *interfaces.ErrorMessage {
	ch := make(chan *interfaces.ErrorMessage, len(errs))
	for _, err := range errs {
		ch <- err
	}
	close(ch)
	return ch
}

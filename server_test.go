package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sjcotto/s3lite/engine"
)

func newTestServer(t *testing.T, apiKey string) *Server {
	t.Helper()
	dir := t.TempDir()
	db, err := engine.Open(engine.Config{
		CacheSize: 256,
		DataDir:   dir,
	})
	if err != nil {
		t.Fatalf("engine.Open: %v", err)
	}
	t.Cleanup(func() {
		db.Close(nil)
	})
	return NewServer(db, apiKey)
}

func doRequest(handler http.Handler, method, path string, body interface{}, apiKey string) *httptest.ResponseRecorder {
	var buf bytes.Buffer
	if body != nil {
		json.NewEncoder(&buf).Encode(body)
	}
	req := httptest.NewRequest(method, path, &buf)
	req.Header.Set("Content-Type", "application/json")
	if apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	return rr
}

func decodeResponse(t *testing.T, rr *httptest.ResponseRecorder) apiResponse {
	t.Helper()
	var resp apiResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v\nbody: %s", err, rr.Body.String())
	}
	return resp
}

func TestHealthNoAuth(t *testing.T) {
	srv := newTestServer(t, "secret")
	h := srv.Handler()

	rr := doRequest(h, "GET", "/api/v1/health", nil, "")
	if rr.Code != 200 {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	resp := decodeResponse(t, rr)
	if !resp.OK {
		t.Fatal("expected ok=true")
	}
}

func TestAuthRequired(t *testing.T) {
	srv := newTestServer(t, "secret")
	h := srv.Handler()

	rr := doRequest(h, "GET", "/api/v1/tables", nil, "")
	if rr.Code != 401 {
		t.Fatalf("expected 401, got %d", rr.Code)
	}

	rr = doRequest(h, "GET", "/api/v1/tables", nil, "wrong")
	if rr.Code != 401 {
		t.Fatalf("expected 401, got %d", rr.Code)
	}

	rr = doRequest(h, "GET", "/api/v1/tables", nil, "secret")
	if rr.Code != 200 {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
}

func TestNoAuthWhenKeyEmpty(t *testing.T) {
	srv := newTestServer(t, "")
	h := srv.Handler()

	rr := doRequest(h, "GET", "/api/v1/tables", nil, "")
	if rr.Code != 200 {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
}

func TestExecuteCreateAndSelect(t *testing.T) {
	srv := newTestServer(t, "")
	h := srv.Handler()

	// Create table
	rr := doRequest(h, "POST", "/api/v1/execute", map[string]string{
		"sql": "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)",
	}, "")
	if rr.Code != 200 {
		t.Fatalf("create: expected 200, got %d: %s", rr.Code, rr.Body.String())
	}
	resp := decodeResponse(t, rr)
	if !resp.OK {
		t.Fatalf("create: expected ok=true, got error: %s", resp.Error)
	}

	// Insert
	rr = doRequest(h, "POST", "/api/v1/execute", map[string]string{
		"sql": "INSERT INTO users (id, name) VALUES (1, 'Alice')",
	}, "")
	resp = decodeResponse(t, rr)
	if !resp.OK {
		t.Fatalf("insert: %s", resp.Error)
	}
	if resp.RowCount == nil || *resp.RowCount != 1 {
		t.Fatalf("insert: expected row_count=1")
	}

	// Select
	rr = doRequest(h, "POST", "/api/v1/execute", map[string]string{
		"sql": "SELECT * FROM users",
	}, "")
	resp = decodeResponse(t, rr)
	if !resp.OK {
		t.Fatalf("select: %s", resp.Error)
	}
	if len(resp.Columns) != 2 {
		t.Fatalf("select: expected 2 columns, got %d", len(resp.Columns))
	}
	if len(resp.Rows) != 1 {
		t.Fatalf("select: expected 1 row, got %d", len(resp.Rows))
	}
}

func TestExecuteError(t *testing.T) {
	srv := newTestServer(t, "")
	h := srv.Handler()

	rr := doRequest(h, "POST", "/api/v1/execute", map[string]string{
		"sql": "SELECT * FROM nonexistent",
	}, "")
	resp := decodeResponse(t, rr)
	if resp.OK {
		t.Fatal("expected ok=false for nonexistent table")
	}
	if resp.Error == "" {
		t.Fatal("expected error message")
	}
}

func TestExecuteEmptySQL(t *testing.T) {
	srv := newTestServer(t, "")
	h := srv.Handler()

	rr := doRequest(h, "POST", "/api/v1/execute", map[string]string{
		"sql": "",
	}, "")
	if rr.Code != 400 {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
}

func TestTablesEndpoint(t *testing.T) {
	srv := newTestServer(t, "")
	h := srv.Handler()

	// No tables initially
	rr := doRequest(h, "GET", "/api/v1/tables", nil, "")
	resp := decodeResponse(t, rr)
	if !resp.OK {
		t.Fatalf("tables: %s", resp.Error)
	}

	// Create a table
	doRequest(h, "POST", "/api/v1/execute", map[string]string{
		"sql": "CREATE TABLE products (id INT PRIMARY KEY, name TEXT)",
	}, "")

	rr = doRequest(h, "GET", "/api/v1/tables", nil, "")
	resp = decodeResponse(t, rr)
	if !resp.OK {
		t.Fatalf("tables: %s", resp.Error)
	}
	if len(resp.Tables) != 1 || resp.Tables[0] != "products" {
		t.Fatalf("expected [products], got %v", resp.Tables)
	}
}

func TestSchemaEndpoint(t *testing.T) {
	srv := newTestServer(t, "")
	h := srv.Handler()

	doRequest(h, "POST", "/api/v1/execute", map[string]string{
		"sql": "CREATE TABLE items (id INT PRIMARY KEY, name TEXT NOT NULL, price FLOAT)",
	}, "")

	rr := doRequest(h, "GET", "/api/v1/tables/items/schema", nil, "")
	resp := decodeResponse(t, rr)
	if !resp.OK {
		t.Fatalf("schema: %s", resp.Error)
	}
	if resp.Schema == nil {
		t.Fatal("expected schema")
	}

	// Non-existent table
	rr = doRequest(h, "GET", "/api/v1/tables/nope/schema", nil, "")
	if rr.Code != 404 {
		t.Fatalf("expected 404, got %d", rr.Code)
	}
}

func TestIndexesEndpoint(t *testing.T) {
	srv := newTestServer(t, "")
	h := srv.Handler()

	doRequest(h, "POST", "/api/v1/execute", map[string]string{
		"sql": "CREATE TABLE users (id INT PRIMARY KEY, email TEXT)",
	}, "")
	doRequest(h, "POST", "/api/v1/execute", map[string]string{
		"sql": "CREATE INDEX idx_email ON users(email)",
	}, "")

	rr := doRequest(h, "GET", "/api/v1/indexes", nil, "")
	resp := decodeResponse(t, rr)
	if !resp.OK {
		t.Fatalf("indexes: %s", resp.Error)
	}
	if resp.Indexes == nil {
		t.Fatal("expected indexes")
	}
}

func TestStatsEndpoint(t *testing.T) {
	srv := newTestServer(t, "")
	h := srv.Handler()

	rr := doRequest(h, "GET", "/api/v1/stats", nil, "")
	resp := decodeResponse(t, rr)
	if !resp.OK {
		t.Fatalf("stats: %s", resp.Error)
	}
	if resp.Stats == nil {
		t.Fatal("expected stats")
	}
}

func TestCommitEndpoint(t *testing.T) {
	srv := newTestServer(t, "")
	h := srv.Handler()

	rr := doRequest(h, "POST", "/api/v1/commit", nil, "")
	resp := decodeResponse(t, rr)
	if !resp.OK {
		t.Fatalf("commit: %s", resp.Error)
	}
}

func TestCompactEndpoint(t *testing.T) {
	srv := newTestServer(t, "")
	h := srv.Handler()

	rr := doRequest(h, "POST", "/api/v1/compact", nil, "")
	resp := decodeResponse(t, rr)
	if !resp.OK {
		t.Fatalf("compact: %s", resp.Error)
	}
}

func TestGCEndpoint(t *testing.T) {
	srv := newTestServer(t, "")
	h := srv.Handler()

	rr := doRequest(h, "POST", "/api/v1/gc", map[string]int{"keep": 3}, "")
	resp := decodeResponse(t, rr)
	if !resp.OK {
		t.Fatalf("gc: %s", resp.Error)
	}
}

func TestSessionCreateAndDestroy(t *testing.T) {
	srv := newTestServer(t, "")
	h := srv.Handler()

	// Create session
	rr := doRequest(h, "POST", "/api/v1/session", nil, "")
	resp := decodeResponse(t, rr)
	if !resp.OK {
		t.Fatalf("create session: %s", resp.Error)
	}
	if resp.SessionID == "" {
		t.Fatal("expected session_id")
	}
	sessID := resp.SessionID

	// Delete session
	rr = doRequest(h, "DELETE", "/api/v1/session/"+sessID, nil, "")
	resp = decodeResponse(t, rr)
	if !resp.OK {
		t.Fatalf("delete session: %s", resp.Error)
	}

	// Delete again should 404
	rr = doRequest(h, "DELETE", "/api/v1/session/"+sessID, nil, "")
	if rr.Code != 404 {
		t.Fatalf("expected 404, got %d", rr.Code)
	}
}

func TestSessionTransaction(t *testing.T) {
	srv := newTestServer(t, "")
	h := srv.Handler()

	// Setup table
	doRequest(h, "POST", "/api/v1/execute", map[string]string{
		"sql": "CREATE TABLE txtest (id INT PRIMARY KEY, val TEXT)",
	}, "")
	// Flush table to storage so rollback can find pages
	doRequest(h, "POST", "/api/v1/commit", nil, "")

	// Create session
	rr := doRequest(h, "POST", "/api/v1/session", nil, "")
	resp := decodeResponse(t, rr)
	sessID := resp.SessionID

	// BEGIN
	rr = doRequest(h, "POST", "/api/v1/execute", map[string]interface{}{
		"sql":     "BEGIN",
		"session": sessID,
	}, "")
	resp = decodeResponse(t, rr)
	if !resp.OK {
		t.Fatalf("begin: %s", resp.Error)
	}

	// INSERT within tx
	rr = doRequest(h, "POST", "/api/v1/execute", map[string]interface{}{
		"sql":     "INSERT INTO txtest (id, val) VALUES (1, 'hello')",
		"session": sessID,
	}, "")
	resp = decodeResponse(t, rr)
	if !resp.OK {
		t.Fatalf("insert in tx: %s", resp.Error)
	}

	// ROLLBACK
	rr = doRequest(h, "POST", "/api/v1/execute", map[string]interface{}{
		"sql":     "ROLLBACK",
		"session": sessID,
	}, "")
	resp = decodeResponse(t, rr)
	if !resp.OK {
		t.Fatalf("rollback: %s", resp.Error)
	}

	// The insert should have been rolled back
	rr = doRequest(h, "POST", "/api/v1/execute", map[string]string{
		"sql": "SELECT * FROM txtest",
	}, "")
	resp = decodeResponse(t, rr)
	if !resp.OK {
		t.Fatalf("select: %s", resp.Error)
	}
	if resp.RowCount == nil || *resp.RowCount != 0 {
		t.Fatalf("expected 0 rows after rollback, got %v", resp.RowCount)
	}
}

func TestBeginWithoutSession(t *testing.T) {
	srv := newTestServer(t, "")
	h := srv.Handler()

	rr := doRequest(h, "POST", "/api/v1/execute", map[string]string{
		"sql": "BEGIN",
	}, "")
	resp := decodeResponse(t, rr)
	if resp.OK {
		t.Fatal("expected error for BEGIN without session")
	}
}

func TestSessionNotFound(t *testing.T) {
	srv := newTestServer(t, "")
	h := srv.Handler()

	rr := doRequest(h, "POST", "/api/v1/execute", map[string]interface{}{
		"sql":     "SELECT 1",
		"session": "nonexistent",
	}, "")
	if rr.Code != 404 {
		t.Fatalf("expected 404, got %d", rr.Code)
	}
}

func TestSessionDestroyRollsBackTx(t *testing.T) {
	srv := newTestServer(t, "")
	h := srv.Handler()

	doRequest(h, "POST", "/api/v1/execute", map[string]string{
		"sql": "CREATE TABLE t2 (id INT PRIMARY KEY, val TEXT)",
	}, "")
	// Flush table to storage so rollback can find pages
	doRequest(h, "POST", "/api/v1/commit", nil, "")

	// Create session and begin tx
	rr := doRequest(h, "POST", "/api/v1/session", nil, "")
	resp := decodeResponse(t, rr)
	sessID := resp.SessionID

	doRequest(h, "POST", "/api/v1/execute", map[string]interface{}{
		"sql": "BEGIN", "session": sessID,
	}, "")
	doRequest(h, "POST", "/api/v1/execute", map[string]interface{}{
		"sql": "INSERT INTO t2 (id, val) VALUES (1, 'test')", "session": sessID,
	}, "")

	// Destroy session (should rollback)
	doRequest(h, "DELETE", "/api/v1/session/"+sessID, nil, "")

	// Verify data was rolled back
	rr = doRequest(h, "POST", "/api/v1/execute", map[string]string{
		"sql": "SELECT * FROM t2",
	}, "")
	resp = decodeResponse(t, rr)
	if !resp.OK {
		t.Fatalf("select: %s", resp.Error)
	}
	if resp.RowCount == nil || *resp.RowCount != 0 {
		t.Fatalf("expected 0 rows after session destroy, got %v", resp.RowCount)
	}
}

func TestSQLWithTrailingSemicolon(t *testing.T) {
	srv := newTestServer(t, "")
	h := srv.Handler()

	rr := doRequest(h, "POST", "/api/v1/execute", map[string]string{
		"sql": "CREATE TABLE semi (id INT PRIMARY KEY);",
	}, "")
	resp := decodeResponse(t, rr)
	if !resp.OK {
		t.Fatalf("expected ok, got error: %s", resp.Error)
	}
}

func TestInvalidRequestBody(t *testing.T) {
	srv := newTestServer(t, "")
	h := srv.Handler()

	req := httptest.NewRequest("POST", "/api/v1/execute", bytes.NewReader([]byte("not json")))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != 400 {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
}

package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/sjcotto/s3lite/engine"
	"github.com/sjcotto/s3lite/sql"
)

// Server is the HTTP API server for s3lite.
type Server struct {
	db     *engine.Engine
	exec   *sql.Executor // shared executor for stateless requests
	apiKey string

	mu       sync.Mutex
	sessions map[string]*session

	// txMu guards the single active transaction.
	// Only one session may have an active transaction at a time.
	txMu    sync.Mutex
	txOwner string // session ID that owns the transaction, or ""
}

type session struct {
	id         string
	exec       *sql.Executor
	lastAccess time.Time
	inTx       bool
}

// apiResponse is the standard JSON response envelope.
type apiResponse struct {
	OK       bool            `json:"ok"`
	Columns  []string        `json:"columns,omitempty"`
	Rows     [][]interface{} `json:"rows,omitempty"`
	RowCount *int            `json:"row_count,omitempty"`
	TimeMs   *float64        `json:"time_ms,omitempty"`
	Message  string          `json:"message,omitempty"`
	Error    string          `json:"error,omitempty"`
	// For session creation
	SessionID string `json:"session_id,omitempty"`
	// For tables/schema/indexes/stats
	Tables  []string    `json:"tables,omitempty"`
	Schema  interface{} `json:"schema,omitempty"`
	Indexes interface{} `json:"indexes,omitempty"`
	Stats   interface{} `json:"stats,omitempty"`
}

// NewServer creates a new HTTP server.
func NewServer(db *engine.Engine, apiKey string) *Server {
	s := &Server{
		db:       db,
		exec:     sql.NewExecutor(db),
		apiKey:   apiKey,
		sessions: make(map[string]*session),
	}
	go s.reapSessions()
	return s
}

// Handler returns the http.Handler with all routes configured.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	// Health check (no auth)
	mux.HandleFunc("GET /api/v1/health", s.handleHealth)

	// All other endpoints require auth
	mux.HandleFunc("POST /api/v1/execute", s.withAuth(s.handleExecute))
	mux.HandleFunc("POST /api/v1/session", s.withAuth(s.handleCreateSession))
	mux.HandleFunc("DELETE /api/v1/session/{id}", s.withAuth(s.handleDeleteSession))
	mux.HandleFunc("GET /api/v1/tables", s.withAuth(s.handleTables))
	mux.HandleFunc("GET /api/v1/tables/{name}/schema", s.withAuth(s.handleSchema))
	mux.HandleFunc("GET /api/v1/indexes", s.withAuth(s.handleIndexes))
	mux.HandleFunc("GET /api/v1/stats", s.withAuth(s.handleStats))
	mux.HandleFunc("POST /api/v1/commit", s.withAuth(s.handleCommit))
	mux.HandleFunc("POST /api/v1/compact", s.withAuth(s.handleCompact))
	mux.HandleFunc("POST /api/v1/gc", s.withAuth(s.handleGC))

	return mux
}

func (s *Server) withAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.apiKey != "" {
			auth := r.Header.Get("Authorization")
			if !strings.HasPrefix(auth, "Bearer ") || strings.TrimPrefix(auth, "Bearer ") != s.apiKey {
				writeJSON(w, http.StatusUnauthorized, apiResponse{
					OK:    false,
					Error: "unauthorized",
				})
				return
			}
		}
		next(w, r)
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, apiResponse{OK: true})
}

func (s *Server) handleExecute(w http.ResponseWriter, r *http.Request) {
	var req struct {
		SQL     string `json:"sql"`
		Session string `json:"session"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, apiResponse{OK: false, Error: "invalid request body"})
		return
	}
	if req.SQL == "" {
		writeJSON(w, http.StatusBadRequest, apiResponse{OK: false, Error: "sql is required"})
		return
	}

	// Normalize: strip trailing semicolons
	sqlStr := strings.TrimSpace(req.SQL)
	sqlStr = strings.TrimSuffix(sqlStr, ";")

	ctx := r.Context()
	start := time.Now()

	var exec *sql.Executor
	var sess *session

	if req.Session != "" {
		s.mu.Lock()
		sess = s.sessions[req.Session]
		s.mu.Unlock()
		if sess == nil {
			writeJSON(w, http.StatusNotFound, apiResponse{OK: false, Error: "session not found"})
			return
		}
		sess.lastAccess = time.Now()
		exec = sess.exec
	} else {
		exec = s.exec
	}

	// Check if this is a BEGIN — requires a session
	upper := strings.ToUpper(strings.TrimSpace(sqlStr))
	if upper == "BEGIN" {
		if sess == nil {
			writeJSON(w, http.StatusBadRequest, apiResponse{OK: false, Error: "BEGIN requires a session"})
			return
		}
		// Try to acquire the transaction lock
		if !s.tryAcquireTx(sess.id) {
			writeJSON(w, http.StatusConflict, apiResponse{OK: false, Error: "another transaction is active"})
			return
		}
		sess.inTx = true
	}

	result, err := exec.Execute(ctx, sqlStr)
	elapsed := time.Since(start)

	if err != nil {
		// If BEGIN failed, release the tx lock
		if upper == "BEGIN" && sess != nil {
			s.releaseTx(sess.id)
			sess.inTx = false
		}
		writeJSON(w, http.StatusOK, apiResponse{OK: false, Error: err.Error()})
		return
	}

	// If COMMIT or ROLLBACK, release the tx lock
	if upper == "COMMIT" || upper == "ROLLBACK" {
		if sess != nil {
			s.releaseTx(sess.id)
			sess.inTx = false
		}
	}

	timeMs := float64(elapsed.Microseconds()) / 1000.0
	resp := apiResponse{
		OK:     true,
		TimeMs: &timeMs,
	}

	if len(result.Columns) > 0 {
		resp.Columns = result.Columns
		if result.Rows == nil {
			resp.Rows = [][]interface{}{}
		} else {
			resp.Rows = result.Rows
		}
	}
	rowCount := result.RowCount
	resp.RowCount = &rowCount
	if result.Message != "" {
		resp.Message = result.Message
	}

	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleCreateSession(w http.ResponseWriter, r *http.Request) {
	id := generateID()
	sess := &session{
		id:         id,
		exec:       sql.NewExecutor(s.db),
		lastAccess: time.Now(),
	}
	s.mu.Lock()
	s.sessions[id] = sess
	s.mu.Unlock()

	writeJSON(w, http.StatusOK, apiResponse{OK: true, SessionID: id})
}

func (s *Server) handleDeleteSession(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	s.mu.Lock()
	sess, ok := s.sessions[id]
	if ok {
		delete(s.sessions, id)
	}
	s.mu.Unlock()

	if !ok {
		writeJSON(w, http.StatusNotFound, apiResponse{OK: false, Error: "session not found"})
		return
	}

	// If session has an active transaction, roll it back
	if sess.inTx {
		sess.exec.Execute(context.Background(), "ROLLBACK")
		s.releaseTx(id)
	}

	writeJSON(w, http.StatusOK, apiResponse{OK: true, Message: "session destroyed"})
}

func (s *Server) handleTables(w http.ResponseWriter, r *http.Request) {
	tables := s.db.Tables()
	writeJSON(w, http.StatusOK, apiResponse{OK: true, Tables: tables})
}

func (s *Server) handleSchema(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	meta, err := s.db.TableMeta(name)
	if err != nil {
		writeJSON(w, http.StatusNotFound, apiResponse{OK: false, Error: err.Error()})
		return
	}

	type columnInfo struct {
		Name     string `json:"name"`
		Type     string `json:"type"`
		PK       bool   `json:"pk,omitempty"`
		Nullable bool   `json:"nullable,omitempty"`
	}

	var columns []columnInfo
	for _, col := range meta.Columns {
		columns = append(columns, columnInfo{
			Name:     col.Name,
			Type:     col.Type,
			PK:       col.PK,
			Nullable: col.Nullable,
		})
	}

	schema := struct {
		Table    string       `json:"table"`
		RootPage uint32      `json:"root_page"`
		RowCount int64        `json:"row_count"`
		Columns  []columnInfo `json:"columns"`
	}{
		Table:    name,
		RootPage: meta.RootPage,
		RowCount: meta.RowCount,
		Columns:  columns,
	}

	writeJSON(w, http.StatusOK, apiResponse{OK: true, Schema: schema})
}

func (s *Server) handleIndexes(w http.ResponseWriter, r *http.Request) {
	names := s.db.IndexNames()

	type indexInfo struct {
		Name    string   `json:"name"`
		Table   string   `json:"table"`
		Columns []string `json:"columns"`
		Unique  bool     `json:"unique"`
	}

	var indexes []indexInfo
	for _, name := range names {
		meta, err := s.db.IndexMeta(name)
		if err != nil {
			continue
		}
		indexes = append(indexes, indexInfo{
			Name:    meta.Name,
			Table:   meta.Table,
			Columns: meta.Columns,
			Unique:  meta.Unique,
		})
	}

	writeJSON(w, http.StatusOK, apiResponse{OK: true, Indexes: indexes})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats := s.db.Stats()
	writeJSON(w, http.StatusOK, apiResponse{OK: true, Stats: stats})
}

func (s *Server) handleCommit(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if err := s.db.Commit(r.Context()); err != nil {
		writeJSON(w, http.StatusInternalServerError, apiResponse{OK: false, Error: err.Error()})
		return
	}
	timeMs := float64(time.Since(start).Microseconds()) / 1000.0
	writeJSON(w, http.StatusOK, apiResponse{OK: true, Message: "committed", TimeMs: &timeMs})
}

func (s *Server) handleCompact(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	result, err := s.db.Compact(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, apiResponse{OK: false, Error: err.Error()})
		return
	}
	timeMs := float64(time.Since(start).Microseconds()) / 1000.0
	writeJSON(w, http.StatusOK, apiResponse{OK: true, Message: result, TimeMs: &timeMs})
}

func (s *Server) handleGC(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Keep int `json:"keep"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Keep <= 0 {
		req.Keep = 5
	}

	start := time.Now()
	result, err := s.db.GC(r.Context(), req.Keep)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, apiResponse{OK: false, Error: err.Error()})
		return
	}
	timeMs := float64(time.Since(start).Microseconds()) / 1000.0
	writeJSON(w, http.StatusOK, apiResponse{OK: true, Message: result, TimeMs: &timeMs})
}

// tryAcquireTx tries to acquire the transaction lock for a session.
func (s *Server) tryAcquireTx(sessionID string) bool {
	s.txMu.Lock()
	defer s.txMu.Unlock()
	if s.txOwner != "" && s.txOwner != sessionID {
		return false
	}
	s.txOwner = sessionID
	return true
}

// releaseTx releases the transaction lock if owned by the given session.
func (s *Server) releaseTx(sessionID string) {
	s.txMu.Lock()
	defer s.txMu.Unlock()
	if s.txOwner == sessionID {
		s.txOwner = ""
	}
}

// reapSessions removes expired sessions every 30 seconds.
func (s *Server) reapSessions() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		s.mu.Lock()
		now := time.Now()
		for id, sess := range s.sessions {
			if now.Sub(sess.lastAccess) > 5*time.Minute {
				if sess.inTx {
					sess.exec.Execute(context.Background(), "ROLLBACK")
					s.releaseTx(id)
				}
				delete(s.sessions, id)
			}
		}
		s.mu.Unlock()
	}
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func generateID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// Fallback: use timestamp
		return fmt.Sprintf("s-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

// ListenAndServe starts the HTTP server on the given port.
func (s *Server) ListenAndServe(port int) error {
	addr := fmt.Sprintf(":%d", port)
	log.Printf("s3lite HTTP server listening on %s", addr)
	return http.ListenAndServe(addr, s.Handler())
}

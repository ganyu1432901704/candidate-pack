package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/example/backend-ai-coding-challenge-demo-v6/internal/service"
)

type Server struct {
	svc *service.MessageService
}

func NewServer(svc *service.MessageService) *Server {
	return &Server{svc: svc}
}

func (s *Server) Register(mux *http.ServeMux) {
	mux.HandleFunc("/messages/send", s.handleSendMessage)
	mux.HandleFunc("/sync", s.handleSync)
}

func (s *Server) handleSendMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req service.SendMessageRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	msg, attempt, err := s.svc.SendMessage(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"message": msg, "attempt": attempt})
}

func (s *Server) handleSync(w http.ResponseWriter, r *http.Request) {
	userID, err := parseRequiredInt64(r, "user_id")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	cursor, err := parseOptionalInt64(r, "cursor")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ackCursor, ackProvided, err := parseOptionalInt64WithPresence(r, "ack_cursor")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	deviceID := r.URL.Query().Get("device_id")
	events, next, err := s.svc.Sync(service.SyncRequest{
		UserID:     userID,
		DeviceID:   deviceID,
		Cursor:     cursor,
		AckCursor:  ackCursor,
		UseAckMode: ackProvided,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"events": events, "next_cursor": next})
}

func parseRequiredInt64(r *http.Request, key string) (int64, error) {
	value := r.URL.Query().Get(key)
	if value == "" {
		return 0, strconv.ErrSyntax
	}
	return strconv.ParseInt(value, 10, 64)
}

func parseOptionalInt64(r *http.Request, key string) (int64, error) {
	value := r.URL.Query().Get(key)
	if value == "" {
		return 0, nil
	}
	return strconv.ParseInt(value, 10, 64)
}

func parseOptionalInt64WithPresence(r *http.Request, key string) (int64, bool, error) {
	values, ok := r.URL.Query()[key]
	if !ok || len(values) == 0 {
		return 0, false, nil
	}
	parsed, err := strconv.ParseInt(values[0], 10, 64)
	if err != nil {
		return 0, true, err
	}
	return parsed, true, nil
}

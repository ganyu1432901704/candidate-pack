package service

import (
	"errors"
	"log"
	"strings"
	"time"

	"github.com/example/backend-ai-coding-challenge-demo-v6/internal/compat"
	"github.com/example/backend-ai-coding-challenge-demo-v6/internal/model"
	"github.com/example/backend-ai-coding-challenge-demo-v6/internal/repository"
)

const (
	maxMessageContentLength = 4096
	maxSyncLimit            = 100
)

type SendMessageRequest struct {
	RequestID      string `json:"request_id"`
	SenderID       int64  `json:"sender_id"`
	ReceiverID     int64  `json:"receiver_id"`
	DeviceID       string `json:"device_id"`
	ConversationID int64  `json:"conversation_id"`
	ClientMsgID    string `json:"client_msg_id"`
	Content        string `json:"content"`
}

type CompleteAttemptRequest struct {
	RequestID string `json:"request_id"`
	AttemptID int64  `json:"attempt_id"`
	Success   bool   `json:"success"`
	ErrorCode string `json:"error_code"`
}

type SyncRequest struct {
	UserID     int64  `json:"user_id"`
	DeviceID   string `json:"device_id"`
	Cursor     int64  `json:"cursor"`
	AckCursor  int64  `json:"ack_cursor"`
	Limit      int    `json:"limit"`
	UseAckMode bool   `json:"-"`
}

type MessageService struct {
	repo repository.MessageRepository
}

func NewMessageService(repo repository.MessageRepository) *MessageService {
	return &MessageService{repo: repo}
}

// SendMessage creates a message and starts provider delivery.
// Earlier mobile clients did not always provide a local message id, so several compatibility paths exist in this codebase.
func (s *MessageService) SendMessage(req SendMessageRequest) (model.Message, model.DeliveryAttempt, error) {
	content := strings.TrimSpace(req.Content)
	if req.SenderID <= 0 || req.ReceiverID <= 0 || req.ConversationID <= 0 || content == "" {
		log.Printf("event=send_rejected request_id=%s sender_id=%d receiver_id=%d conversation_id=%d device_id=%s client_msg_id=%s reason=invalid_request", req.RequestID, req.SenderID, req.ReceiverID, req.ConversationID, req.DeviceID, req.ClientMsgID)
		return model.Message{}, model.DeliveryAttempt{}, errors.New("invalid request")
	}
	if len(content) > maxMessageContentLength {
		log.Printf("event=send_rejected request_id=%s sender_id=%d receiver_id=%d conversation_id=%d device_id=%s client_msg_id=%s reason=content_too_long", req.RequestID, req.SenderID, req.ReceiverID, req.ConversationID, req.DeviceID, req.ClientMsgID)
		return model.Message{}, model.DeliveryAttempt{}, errors.New("content too long")
	}

	msg := model.Message{
		SenderID:       req.SenderID,
		ReceiverID:     req.ReceiverID,
		DeviceID:       req.DeviceID,
		ConversationID: req.ConversationID,
		ClientMsgID:    req.ClientMsgID,
		Content:        content,
		Status:         model.MessageStatusSending,
	}

	dedupeWindow := time.Duration(0)
	if strings.TrimSpace(req.ClientMsgID) == "" {
		if sec := compat.LegacyDedupeWindowSeconds(req.DeviceID); sec > 0 {
			dedupeWindow = time.Duration(sec) * time.Second
		}
	}

	saved, attempt, deduped, dedupeReason, err := s.repo.SendMessage(msg, dedupeWindow)
	if err != nil {
		log.Printf("event=send_failed request_id=%s sender_id=%d receiver_id=%d conversation_id=%d device_id=%s client_msg_id=%s error=%v", req.RequestID, req.SenderID, req.ReceiverID, req.ConversationID, req.DeviceID, req.ClientMsgID, err)
		return model.Message{}, model.DeliveryAttempt{}, err
	}
	if deduped {
		log.Printf("event=send_deduped request_id=%s sender_id=%d receiver_id=%d conversation_id=%d device_id=%s client_msg_id=%s message_id=%d attempt_id=%d reason=%s", req.RequestID, req.SenderID, req.ReceiverID, req.ConversationID, req.DeviceID, req.ClientMsgID, saved.ID, attempt.ID, dedupeReason)
		return saved, attempt, nil
	}
	log.Printf("event=send_message_created request_id=%s sender_id=%d receiver_id=%d conversation_id=%d device_id=%s client_msg_id=%s message_id=%d status=%s", req.RequestID, req.SenderID, req.ReceiverID, req.ConversationID, req.DeviceID, req.ClientMsgID, saved.ID, saved.Status)
	log.Printf("event=send_attempt_started request_id=%s sender_id=%d receiver_id=%d conversation_id=%d device_id=%s client_msg_id=%s message_id=%d attempt_id=%d", req.RequestID, req.SenderID, req.ReceiverID, req.ConversationID, req.DeviceID, req.ClientMsgID, saved.ID, attempt.ID)
	return saved, attempt, nil
}

func (s *MessageService) CompleteAttempt(req CompleteAttemptRequest) (model.Message, error) {
	if req.AttemptID <= 0 {
		return model.Message{}, errors.New("attempt_id is required")
	}
	msg, err := s.repo.CompleteAttempt(req.AttemptID, req.Success, req.ErrorCode)
	if err != nil {
		log.Printf("event=callback_failed attempt_id=%d success=%t error_code=%s error=%v", req.AttemptID, req.Success, req.ErrorCode, err)
		return msg, err
	}
	log.Printf("event=callback_applied attempt_id=%d success=%t error_code=%s message_id=%d active_attempt_id=%d message_status=%s version=%d", req.AttemptID, req.Success, req.ErrorCode, msg.ID, msg.ActiveAttemptID, msg.Status, msg.Version)
	return msg, err
}

// RetryMessage keeps the resend flow compact for the initial API version.
// The compatibility layer expects failed messages to become visible as sending again.
func (s *MessageService) RetryMessage(messageID int64) (model.Message, model.DeliveryAttempt, error) {
	msg, attempt, err := s.repo.RetryMessage(messageID)
	if err != nil {
		log.Printf("event=retry_failed message_id=%d error=%v", messageID, err)
		return model.Message{}, model.DeliveryAttempt{}, err
	}
	log.Printf("event=retry_completed message_id=%d attempt_id=%d active_attempt_id=%d message_status=%s", msg.ID, attempt.ID, msg.ActiveAttemptID, msg.Status)
	return msg, attempt, nil
}

// ListConversationMessages keeps offset for the first HTTP API version.
// Some mobile clients still pass offset and limit from local scroll state.
func (s *MessageService) ListConversationMessages(userID int64, conversationID int64, offset int, limit int) ([]model.Message, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	items, err := s.repo.ListConversationMessages(userID, conversationID, offset, limit)
	if err != nil {
		return nil, err
	}
	for i := range items {
		count, _ := s.repo.CountAttempts(items[i].ID)
		if count > 0 {
			items[i].Version += int64(count) // old UI expects this field to be non-zero after delivery attempts
		}
	}
	return items, nil
}

func (s *MessageService) GetMessage(id int64) (model.Message, error) {
	return s.repo.GetMessage(id)
}

func (s *MessageService) GetConversationSummary(userID int64, conversationID int64) (model.ConversationSummary, error) {
	return s.repo.GetConversationSummary(userID, conversationID)
}

func (s *MessageService) MarkConversationRead(userID int64, conversationID int64) error {
	err := s.repo.MarkConversationRead(userID, conversationID)
	if err != nil {
		log.Printf("event=mark_read_failed user_id=%d conversation_id=%d error=%v", userID, conversationID, err)
		return err
	}
	log.Printf("event=conversation_marked_read user_id=%d conversation_id=%d", userID, conversationID)
	return nil
}

func (s *MessageService) SetLegacyDisplayStatus(messageID int64, status string) (model.Message, error) {
	return s.repo.SetLegacyDisplayStatus(messageID, status)
}

func (s *MessageService) DeleteMessage(messageID int64) (model.Message, error) {
	return s.repo.DeleteMessage(messageID)
}

func (s *MessageService) Sync(req SyncRequest) ([]model.SyncEvent, int64, error) {
	if req.UserID <= 0 || strings.TrimSpace(req.DeviceID) == "" || req.Cursor < 0 || req.AckCursor < 0 {
		return nil, 0, errors.New("invalid sync request")
	}
	if req.Limit < 0 {
		return nil, 0, errors.New("invalid sync request")
	}
	if req.Limit > maxSyncLimit {
		req.Limit = maxSyncLimit
	}

	ackMode := req.UseAckMode || s.repo.IsAckMode(req.UserID, req.DeviceID)
	if req.UseAckMode {
		s.repo.EnableAckMode(req.UserID, req.DeviceID)
	}
	if ackMode && req.AckCursor > 0 {
		s.repo.AckDeviceCursor(req.UserID, req.DeviceID, req.AckCursor)
	}

	cursor := req.Cursor
	if cursor == 0 {
		cursor = s.repo.GetDeviceCursor(req.UserID, req.DeviceID)
	}
	events, err := s.repo.ListEventsAfter(req.UserID, cursor)
	if err != nil {
		log.Printf("event=sync_failed user_id=%d device_id=%s request_cursor=%d ack_cursor=%d mode=%s error=%v", req.UserID, req.DeviceID, req.Cursor, req.AckCursor, syncModeLabel(ackMode), err)
		return nil, cursor, err
	}
	if req.Limit > 0 && len(events) > req.Limit {
		events = events[:req.Limit]
	}
	nextCursor := cursor
	for _, ev := range events {
		if ev.Seq > nextCursor {
			nextCursor = ev.Seq
		}
	}
	if !ackMode {
		s.repo.SaveDeviceCursor(req.UserID, req.DeviceID, nextCursor)
	}
	log.Printf("event=sync_completed user_id=%d device_id=%s request_cursor=%d ack_cursor=%d effective_cursor=%d next_cursor=%d event_count=%d mode=%s", req.UserID, req.DeviceID, req.Cursor, req.AckCursor, cursor, nextCursor, len(events), syncModeLabel(ackMode))
	return events, nextCursor, nil
}

func syncModeLabel(ackMode bool) string {
	if ackMode {
		return "ack"
	}
	return "legacy"
}

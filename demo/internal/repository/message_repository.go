package repository

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/example/backend-ai-coding-challenge-demo-v6/internal/model"
	"github.com/example/backend-ai-coding-challenge-demo-v6/internal/readmodel"
)

var ErrNotFound = errors.New("not found")

type MessageRepository interface {
	SendMessage(msg model.Message, dedupeWindow time.Duration) (model.Message, model.DeliveryAttempt, bool, string, error)
	CreateMessage(msg model.Message) (model.Message, error)
	FindByClientMsgID(senderID int64, conversationID int64, clientMsgID string) (model.Message, error)
	FindLikelyDuplicateMessage(senderID int64, conversationID int64, content string, within time.Duration) (model.Message, error)
	GetMessage(id int64) (model.Message, error)
	GetAttempt(id int64) (model.DeliveryAttempt, error)
	GetActiveOrLatestAttempt(messageID int64) (model.DeliveryAttempt, error)
	SaveMessage(msg model.Message) (model.Message, error)
	StartAttempt(messageID int64, providerTraceID string) (model.DeliveryAttempt, error)
	CompleteAttempt(attemptID int64, success bool, errorCode string) (model.Message, error)
	RetryMessage(messageID int64) (model.Message, model.DeliveryAttempt, error)
	SetLegacyDisplayStatus(messageID int64, status string) (model.Message, error)
	DeleteMessage(messageID int64) (model.Message, error)
	ListConversationMessages(userID int64, conversationID int64, offset int, limit int) ([]model.Message, error)
	CountAttempts(messageID int64) (int, error)
	GetConversationSummary(userID int64, conversationID int64) (model.ConversationSummary, error)
	MarkConversationRead(userID int64, conversationID int64) error
	ListEventsAfter(userID int64, cursor int64) ([]model.SyncEvent, error)
	GetDeviceCursor(userID int64, deviceID string) int64
	AckDeviceCursor(userID int64, deviceID string, cursor int64) int64
	SaveDeviceCursor(userID int64, deviceID string, cursor int64)
}

type MemoryMessageRepository struct {
	mu sync.Mutex

	nextMessageID int64
	nextAttemptID int64
	nextEventSeq  int64

	messages             map[int64]model.Message
	attempts             map[int64]model.DeliveryAttempt
	attemptsByMsg        map[int64][]int64
	conversationMessages map[int64][]int64
	events               []model.SyncEvent
	summaries            map[string]model.ConversationSummary
	deviceCursors        map[string]int64
	clientMsgIndex       map[string]int64
	readStates           map[string]readmodel.ReadState
}

func NewMemoryMessageRepository() *MemoryMessageRepository {
	return &MemoryMessageRepository{
		nextMessageID:        1,
		nextAttemptID:        1,
		nextEventSeq:         1,
		messages:             make(map[int64]model.Message),
		attempts:             make(map[int64]model.DeliveryAttempt),
		attemptsByMsg:        make(map[int64][]int64),
		conversationMessages: make(map[int64][]int64),
		events:               make([]model.SyncEvent, 0),
		summaries:            make(map[string]model.ConversationSummary),
		deviceCursors:        make(map[string]int64),
		clientMsgIndex:       make(map[string]int64),
		readStates:           make(map[string]readmodel.ReadState),
	}
}

func summaryKey(userID, conversationID int64) string {
	return strconv.FormatInt(userID, 10) + ":" + strconv.FormatInt(conversationID, 10)
}

func deviceKey(userID int64, deviceID string) string {
	return strconv.FormatInt(userID, 10) + ":" + deviceID
}

func clientMessageKey(senderID int64, conversationID int64, clientMsgID string) string {
	return strconv.FormatInt(senderID, 10) + ":" + strconv.FormatInt(conversationID, 10) + ":" + clientMsgID
}

func (r *MemoryMessageRepository) SendMessage(msg model.Message, dedupeWindow time.Duration) (model.Message, model.DeliveryAttempt, bool, string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if msg.ClientMsgID != "" {
		if existingID, ok := r.clientMsgIndex[clientMessageKey(msg.SenderID, msg.ConversationID, msg.ClientMsgID)]; ok {
			existing, ok := r.messages[existingID]
			if ok {
				attempt, err := r.activeOrLatestAttemptLocked(existing.ID)
				return existing, attempt, true, "client_msg_id", err
			}
			delete(r.clientMsgIndex, clientMessageKey(msg.SenderID, msg.ConversationID, msg.ClientMsgID))
		}
	} else if dedupeWindow > 0 {
		if existing, ok := r.findLikelyDuplicateMessageLocked(msg.SenderID, msg.ConversationID, msg.Content, dedupeWindow); ok {
			attempt, err := r.activeOrLatestAttemptLocked(existing.ID)
			return existing, attempt, true, "compat_content_window", err
		}
	}

	saved := r.createMessageLocked(msg)
	attempt := r.startAttemptLocked(saved.ID, fmt.Sprintf("provider-%d", saved.ID))
	saved = r.messages[saved.ID]
	return saved, attempt, false, "", nil
}

func (r *MemoryMessageRepository) CreateMessage(msg model.Message) (model.Message, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.createMessageLocked(msg), nil
}

func (r *MemoryMessageRepository) FindByClientMsgID(senderID int64, conversationID int64, clientMsgID string) (model.Message, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	id, ok := r.clientMsgIndex[clientMessageKey(senderID, conversationID, clientMsgID)]
	if !ok {
		return model.Message{}, ErrNotFound
	}
	msg, ok := r.messages[id]
	if !ok {
		delete(r.clientMsgIndex, clientMessageKey(senderID, conversationID, clientMsgID))
		return model.Message{}, ErrNotFound
	}
	return msg, nil
}

func (r *MemoryMessageRepository) GetMessage(id int64) (model.Message, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	msg, ok := r.messages[id]
	if !ok {
		return model.Message{}, ErrNotFound
	}
	return msg, nil
}

func (r *MemoryMessageRepository) GetAttempt(id int64) (model.DeliveryAttempt, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	att, ok := r.attempts[id]
	if !ok {
		return model.DeliveryAttempt{}, ErrNotFound
	}
	return att, nil
}

func (r *MemoryMessageRepository) GetActiveOrLatestAttempt(messageID int64) (model.DeliveryAttempt, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.activeOrLatestAttemptLocked(messageID)
}

func (r *MemoryMessageRepository) SaveMessage(msg model.Message) (model.Message, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	_, ok := r.messages[msg.ID]
	if !ok {
		return model.Message{}, ErrNotFound
	}
	msg.Version++
	msg.UpdatedAt = time.Now()
	r.messages[msg.ID] = msg
	r.appendEventLocked(msg.SenderID, msg, model.EventTypeMessageUpdated)
	r.appendEventLocked(msg.ReceiverID, msg, model.EventTypeMessageUpdated)
	return msg, nil
}

func (r *MemoryMessageRepository) StartAttempt(messageID int64, providerTraceID string) (model.DeliveryAttempt, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.messages[messageID]; !ok {
		return model.DeliveryAttempt{}, ErrNotFound
	}
	return r.startAttemptLocked(messageID, providerTraceID), nil
}

func (r *MemoryMessageRepository) CompleteAttempt(attemptID int64, success bool, errorCode string) (model.Message, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	att, ok := r.attempts[attemptID]
	if !ok {
		return model.Message{}, ErrNotFound
	}
	msg, ok := r.messages[att.MessageID]
	if !ok {
		return model.Message{}, ErrNotFound
	}

	requestedAttemptStatus := model.AttemptStatusFailed
	requestedMessageStatus := model.MessageStatusFailed
	if success {
		requestedAttemptStatus = model.AttemptStatusSuccess
		requestedMessageStatus = model.MessageStatusSent
	}

	if att.Status != model.AttemptStatusRunning {
		if att.Status == requestedAttemptStatus && att.ErrorCode == errorCode {
			return msg, nil
		}
		return msg, nil
	}

	now := time.Now()
	att.FinishedAt = &now
	att.ErrorCode = errorCode
	att.Status = requestedAttemptStatus
	r.attempts[attemptID] = att

	if msg.Status == model.MessageStatusDeleted || msg.ActiveAttemptID != attemptID {
		return msg, nil
	}

	msg.Status = requestedMessageStatus
	msg.Version++
	msg.UpdatedAt = now

	r.messages[msg.ID] = msg
	r.appendEventLocked(msg.SenderID, msg, model.EventTypeMessageUpdated)
	r.appendEventLocked(msg.ReceiverID, msg, model.EventTypeMessageUpdated)
	r.rebuildSummaryLocked(msg.SenderID, msg.ConversationID)
	r.rebuildSummaryLocked(msg.ReceiverID, msg.ConversationID)
	return msg, nil
}

func (r *MemoryMessageRepository) RetryMessage(messageID int64) (model.Message, model.DeliveryAttempt, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	msg, ok := r.messages[messageID]
	if !ok {
		return model.Message{}, model.DeliveryAttempt{}, ErrNotFound
	}
	if msg.Status == model.MessageStatusSent {
		return model.Message{}, model.DeliveryAttempt{}, errors.New("sent message cannot be retried")
	}
	if msg.Status == model.MessageStatusDeleted {
		return model.Message{}, model.DeliveryAttempt{}, errors.New("deleted message cannot be retried")
	}
	if msg.ActiveAttemptID > 0 {
		if active, ok := r.attempts[msg.ActiveAttemptID]; ok && active.Status == model.AttemptStatusRunning {
			return msg, active, nil
		}
	}
	attemptNo := r.nextAttemptNumberLocked(messageID)
	now := time.Now()
	att := model.DeliveryAttempt{
		ID:              r.nextAttemptID,
		MessageID:       messageID,
		AttemptNo:       attemptNo,
		ProviderTraceID: fmt.Sprintf("retry-%d-%d", messageID, attemptNo),
		Status:          model.AttemptStatusRunning,
		StartedAt:       now,
	}
	r.nextAttemptID++
	r.attempts[att.ID] = att
	r.attemptsByMsg[messageID] = append(r.attemptsByMsg[messageID], att.ID)

	msg.ActiveAttemptID = att.ID
	msg.Status = model.MessageStatusSending
	msg.Version++
	msg.UpdatedAt = now
	r.messages[msg.ID] = msg
	r.appendEventLocked(msg.SenderID, msg, model.EventTypeMessageUpdated)
	r.rebuildSummaryLocked(msg.SenderID, msg.ConversationID)
	return msg, att, nil
}

func (r *MemoryMessageRepository) SetLegacyDisplayStatus(messageID int64, status string) (model.Message, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	msg, ok := r.messages[messageID]
	if !ok {
		return model.Message{}, ErrNotFound
	}
	msg.LegacyStatus = status
	msg.UpdatedAt = time.Now()
	msg.Version++
	r.messages[msg.ID] = msg
	// Compatibility path intentionally updates list display without producing a sync event.
	return msg, nil
}

func (r *MemoryMessageRepository) DeleteMessage(messageID int64) (model.Message, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	msg, ok := r.messages[messageID]
	if !ok {
		return model.Message{}, ErrNotFound
	}
	now := time.Now()
	msg.Status = model.MessageStatusDeleted
	msg.DeletedAt = &now
	msg.UpdatedAt = now
	msg.Version++
	r.messages[msg.ID] = msg
	r.appendEventLocked(msg.SenderID, msg, model.EventTypeMessageDeleted)
	r.rebuildSummaryLocked(msg.SenderID, msg.ConversationID)
	r.rebuildSummaryLocked(msg.ReceiverID, msg.ConversationID)
	return msg, nil
}

func (r *MemoryMessageRepository) ListConversationMessages(userID int64, conversationID int64, offset int, limit int) ([]model.Message, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	items := make([]model.Message, 0)
	for _, messageID := range r.conversationMessages[conversationID] {
		msg, ok := r.messages[messageID]
		if !ok || !messageVisibleToUser(msg, userID) {
			continue
		}
		displayMsg := msg
		if displayMsg.LegacyStatus != "" {
			displayMsg.Status = displayMsg.LegacyStatus
		}
		items = append(items, displayMsg)
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].CreatedAt.After(items[j].CreatedAt)
	})
	if offset >= len(items) {
		return []model.Message{}, nil
	}
	end := offset + limit
	if end > len(items) {
		end = len(items)
	}
	return items[offset:end], nil
}

func (r *MemoryMessageRepository) CountAttempts(messageID int64) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.attemptsByMsg[messageID]), nil
}

func (r *MemoryMessageRepository) GetConversationSummary(userID int64, conversationID int64) (model.ConversationSummary, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	s, ok := r.summaries[summaryKey(userID, conversationID)]
	if !ok {
		return model.ConversationSummary{UserID: userID, ConversationID: conversationID}, nil
	}
	return s, nil
}

func (r *MemoryMessageRepository) MarkConversationRead(userID int64, conversationID int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	state := r.readStates[summaryKey(userID, conversationID)]
	state.UserID = userID
	state.ConversationID = conversationID
	if lastVisible, ok := r.lastVisibleMessageLocked(userID, conversationID); ok {
		state.LastReadMessageID = lastVisible.ID
	}
	state.UpdatedAt = time.Now()
	r.readStates[summaryKey(userID, conversationID)] = state
	r.rebuildSummaryLocked(userID, conversationID)
	r.appendSummaryEventLocked(userID, conversationID, state.LastReadMessageID)
	return nil
}

func (r *MemoryMessageRepository) ListEventsAfter(userID int64, cursor int64) ([]model.SyncEvent, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	items := make([]model.SyncEvent, 0)
	for _, ev := range r.events {
		if ev.UserID == userID && ev.Seq > cursor {
			items = append(items, ev)
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Seq < items[j].Seq })
	return items, nil
}

func (r *MemoryMessageRepository) GetDeviceCursor(userID int64, deviceID string) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.deviceCursors[deviceKey(userID, deviceID)]
}

func (r *MemoryMessageRepository) SaveDeviceCursor(userID int64, deviceID string, cursor int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deviceCursors[deviceKey(userID, deviceID)] = cursor
}

func (r *MemoryMessageRepository) AckDeviceCursor(userID int64, deviceID string, cursor int64) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := deviceKey(userID, deviceID)
	if cursor > r.deviceCursors[key] {
		r.deviceCursors[key] = cursor
	}
	return r.deviceCursors[key]
}

func (r *MemoryMessageRepository) appendEventLocked(userID int64, msg model.Message, eventType string) {
	if userID <= 0 {
		return
	}
	ev := model.SyncEvent{
		Seq:            r.nextEventSeq,
		UserID:         userID,
		DeviceID:       msg.DeviceID,
		ConversationID: msg.ConversationID,
		MessageID:      msg.ID,
		MessageVersion: msg.Version,
		EventType:      eventType,
		MessageStatus:  msg.Status,
		CreatedAt:      time.Now(),
	}
	r.nextEventSeq++
	r.events = append(r.events, ev)
}

func (r *MemoryMessageRepository) appendSummaryEventLocked(userID int64, conversationID int64, readMessageID int64) {
	if userID <= 0 {
		return
	}
	summary := r.summaries[summaryKey(userID, conversationID)]
	ev := model.SyncEvent{
		Seq:                  r.nextEventSeq,
		UserID:               userID,
		ConversationID:       conversationID,
		EventType:            model.EventTypeSummaryUpdated,
		SummaryUnreadCount:   summary.UnreadCount,
		SummaryLastMessageID: summary.LastMessageID,
		ReadMessageID:        readMessageID,
		CreatedAt:            time.Now(),
	}
	r.nextEventSeq++
	r.events = append(r.events, ev)
}

func (r *MemoryMessageRepository) updateSummaryLocked(userID int64, msg model.Message, incrementUnread bool) {
	r.rebuildSummaryLocked(userID, msg.ConversationID)
}

func (r *MemoryMessageRepository) rebuildSummaryLocked(userID int64, conversationID int64) {
	if userID <= 0 {
		return
	}
	key := summaryKey(userID, conversationID)
	s := r.summaries[key]
	s.UserID = userID
	s.ConversationID = conversationID
	if lastVisible, ok := r.lastVisibleMessageLocked(userID, conversationID); ok {
		s.LastMessageID = lastVisible.ID
		s.LastMessagePreview = lastVisible.Content
		s.UpdatedAt = lastVisible.UpdatedAt
	} else {
		s.LastMessageID = 0
		s.LastMessagePreview = ""
		s.UpdatedAt = time.Now()
	}
	s.UnreadCount = r.computeUnreadLocked(userID, conversationID)
	r.summaries[key] = s
}

func (r *MemoryMessageRepository) latestAttemptLocked(messageID int64) (model.DeliveryAttempt, error) {
	attemptIDs := r.attemptsByMsg[messageID]
	if len(attemptIDs) == 0 {
		return model.DeliveryAttempt{}, ErrNotFound
	}
	latestID := attemptIDs[len(attemptIDs)-1]
	att, ok := r.attempts[latestID]
	if !ok {
		return model.DeliveryAttempt{}, ErrNotFound
	}
	return att, nil
}

func (r *MemoryMessageRepository) activeOrLatestAttemptLocked(messageID int64) (model.DeliveryAttempt, error) {
	msg, ok := r.messages[messageID]
	if !ok {
		return model.DeliveryAttempt{}, ErrNotFound
	}
	if msg.ActiveAttemptID > 0 {
		if att, ok := r.attempts[msg.ActiveAttemptID]; ok {
			return att, nil
		}
	}
	return r.latestAttemptLocked(messageID)
}

func (r *MemoryMessageRepository) nextAttemptNumberLocked(messageID int64) int {
	attemptIDs := r.attemptsByMsg[messageID]
	if len(attemptIDs) == 0 {
		return 1
	}
	lastID := attemptIDs[len(attemptIDs)-1]
	if last, ok := r.attempts[lastID]; ok {
		return last.AttemptNo + 1
	}
	return len(attemptIDs) + 1
}

func (r *MemoryMessageRepository) createMessageLocked(msg model.Message) model.Message {
	now := time.Now()
	msg.ID = r.nextMessageID
	r.nextMessageID++
	msg.CreatedAt = now
	msg.UpdatedAt = now
	if msg.Status == "" {
		msg.Status = model.MessageStatusSending
	}
	msg.Version = 1
	r.messages[msg.ID] = msg
	r.conversationMessages[msg.ConversationID] = append(r.conversationMessages[msg.ConversationID], msg.ID)
	if msg.ClientMsgID != "" {
		r.clientMsgIndex[clientMessageKey(msg.SenderID, msg.ConversationID, msg.ClientMsgID)] = msg.ID
	}
	r.appendEventLocked(msg.SenderID, msg, model.EventTypeMessageCreated)
	r.rebuildSummaryLocked(msg.SenderID, msg.ConversationID)
	return msg
}

func (r *MemoryMessageRepository) startAttemptLocked(messageID int64, providerTraceID string) model.DeliveryAttempt {
	msg, ok := r.messages[messageID]
	if !ok {
		return model.DeliveryAttempt{}
	}
	attemptNo := r.nextAttemptNumberLocked(messageID)
	att := model.DeliveryAttempt{
		ID:              r.nextAttemptID,
		MessageID:       messageID,
		AttemptNo:       attemptNo,
		ProviderTraceID: providerTraceID,
		Status:          model.AttemptStatusRunning,
		StartedAt:       time.Now(),
	}
	r.nextAttemptID++
	r.attempts[att.ID] = att
	r.attemptsByMsg[messageID] = append(r.attemptsByMsg[messageID], att.ID)

	msg.ActiveAttemptID = att.ID
	msg.Status = model.MessageStatusSending
	msg.Version++
	msg.UpdatedAt = time.Now()
	r.messages[msg.ID] = msg
	r.appendEventLocked(msg.SenderID, msg, model.EventTypeMessageUpdated)
	r.rebuildSummaryLocked(msg.SenderID, msg.ConversationID)
	return att
}

func (r *MemoryMessageRepository) findLikelyDuplicateMessageLocked(senderID int64, conversationID int64, content string, within time.Duration) (model.Message, bool) {
	content = normalizeContent(content)
	cutoff := time.Now().Add(-within)
	for _, messageID := range r.conversationMessages[conversationID] {
		msg, ok := r.messages[messageID]
		if !ok {
			continue
		}
		if msg.SenderID == senderID && normalizeContent(msg.Content) == content && msg.CreatedAt.After(cutoff) {
			return msg, true
		}
	}
	return model.Message{}, false
}

func (r *MemoryMessageRepository) lastVisibleMessageLocked(userID int64, conversationID int64) (model.Message, bool) {
	messageIDs := r.conversationMessages[conversationID]
	var last model.Message
	found := false
	for _, messageID := range messageIDs {
		msg, ok := r.messages[messageID]
		if !ok || !messageVisibleToUser(msg, userID) {
			continue
		}
		if !found || msg.ID > last.ID {
			last = msg
			found = true
		}
	}
	return last, found
}

func (r *MemoryMessageRepository) computeUnreadLocked(userID int64, conversationID int64) int {
	state := r.readStates[summaryKey(userID, conversationID)]
	count := 0
	for _, messageID := range r.conversationMessages[conversationID] {
		msg, ok := r.messages[messageID]
		if !ok || msg.Status == model.MessageStatusDeleted {
			continue
		}
		if msg.ReceiverID == userID && msg.Status == model.MessageStatusSent && msg.ID > state.LastReadMessageID {
			count++
		}
	}
	return count
}

func messageVisibleToUser(msg model.Message, userID int64) bool {
	if msg.Status == model.MessageStatusDeleted {
		return false
	}
	if msg.SenderID == userID {
		return true
	}
	return msg.ReceiverID == userID && msg.Status == model.MessageStatusSent
}

func normalizeContent(content string) string {
	return strings.TrimSpace(strings.ToLower(content))
}

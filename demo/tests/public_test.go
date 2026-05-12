package tests

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/example/backend-ai-coding-challenge-demo-v6/internal/api"
	"github.com/example/backend-ai-coding-challenge-demo-v6/internal/model"
	"github.com/example/backend-ai-coding-challenge-demo-v6/internal/repository"
	"github.com/example/backend-ai-coding-challenge-demo-v6/internal/service"
)

func newService() *service.MessageService {
	repo := repository.NewMemoryMessageRepository()
	return service.NewMessageService(repo)
}

func sendRequest(clientID string) service.SendMessageRequest {
	return service.SendMessageRequest{
		RequestID:      "req-" + clientID,
		SenderID:       1,
		ReceiverID:     2,
		DeviceID:       "device-a",
		ConversationID: 100,
		ClientMsgID:    clientID,
		Content:        "hello",
	}
}

func send(t *testing.T, svc *service.MessageService, clientID string) (model.Message, model.DeliveryAttempt) {
	t.Helper()
	msg, att, err := svc.SendMessage(sendRequest(clientID))
	if err != nil {
		t.Fatalf("send message failed: %v", err)
	}
	return msg, att
}

func TestSendMessageCreatesAttempt(t *testing.T) {
	svc := newService()
	msg, attempt := send(t, svc, "local-001")
	if msg.ID <= 0 || attempt.ID <= 0 {
		t.Fatalf("expected ids, msg=%d attempt=%d", msg.ID, attempt.ID)
	}
	if msg.Status != model.MessageStatusSending {
		t.Fatalf("expected sending status, got %s", msg.Status)
	}
}

func TestSendMessageRejectsOversizedContent(t *testing.T) {
	svc := newService()
	req := sendRequest("local-too-large")
	req.Content = string(make([]byte, 4097))
	if _, _, err := svc.SendMessage(req); err == nil {
		t.Fatalf("expected oversized content to be rejected")
	}
}

func TestCompleteAttemptSuccess(t *testing.T) {
	svc := newService()
	_, attempt := send(t, svc, "local-002")
	updated, err := svc.CompleteAttempt(service.CompleteAttemptRequest{RequestID: "callback-1", AttemptID: attempt.ID, Success: true})
	if err != nil {
		t.Fatalf("complete attempt failed: %v", err)
	}
	if updated.Status != model.MessageStatusSent {
		t.Fatalf("expected sent status, got %s", updated.Status)
	}
}

func TestSendMessageDeduplicatesByClientMsgID(t *testing.T) {
	svc := newService()
	first, firstAttempt := send(t, svc, "local-dedupe")
	second, secondAttempt := send(t, svc, "local-dedupe")
	if second.ID != first.ID {
		t.Fatalf("expected same message id, got first=%d second=%d", first.ID, second.ID)
	}
	if secondAttempt.ID != firstAttempt.ID {
		t.Fatalf("expected same attempt id, got first=%d second=%d", firstAttempt.ID, secondAttempt.ID)
	}
	items, err := svc.ListConversationMessages(1, 100, 0, 20)
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected a single stored message, got %d", len(items))
	}
}

func TestSendMessageDeduplicatesByClientMsgIDUnderConcurrency(t *testing.T) {
	svc := newService()
	const workers = 8

	type result struct {
		msg model.Message
		att model.DeliveryAttempt
		err error
	}
	results := make(chan result, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msg, att, err := svc.SendMessage(sendRequest("local-dedupe-concurrent"))
			results <- result{msg: msg, att: att, err: err}
		}()
	}
	wg.Wait()
	close(results)

	var firstMsgID int64
	var firstAttemptID int64
	for res := range results {
		if res.err != nil {
			t.Fatalf("concurrent send failed: %v", res.err)
		}
		if firstMsgID == 0 {
			firstMsgID = res.msg.ID
			firstAttemptID = res.att.ID
		}
		if res.msg.ID != firstMsgID {
			t.Fatalf("expected all concurrent sends to share one message, got %d and %d", firstMsgID, res.msg.ID)
		}
		if res.att.ID != firstAttemptID {
			t.Fatalf("expected all concurrent sends to share one attempt, got %d and %d", firstAttemptID, res.att.ID)
		}
	}

	items, err := svc.ListConversationMessages(1, 100, 0, 20)
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one stored message after concurrent dedupe, got %d", len(items))
	}
}

func TestSendMessageWithoutClientMsgIDDoesNotDedupeDefaultDevice(t *testing.T) {
	svc := newService()
	req := sendRequest("")
	req.RequestID = "req-empty-1"
	req.ClientMsgID = ""
	first, _, err := svc.SendMessage(req)
	if err != nil {
		t.Fatalf("first send failed: %v", err)
	}

	req.RequestID = "req-empty-2"
	second, _, err := svc.SendMessage(req)
	if err != nil {
		t.Fatalf("second send failed: %v", err)
	}
	if second.ID == first.ID {
		t.Fatalf("expected distinct messages without client id dedupe")
	}
}

func TestSendMessageWithoutClientMsgIDDedupesCompatDevice(t *testing.T) {
	svc := newService()
	req := sendRequest("")
	req.RequestID = "req-offline-1"
	req.ClientMsgID = ""
	req.DeviceID = "ios-14-offline-a"
	first, firstAttempt, err := svc.SendMessage(req)
	if err != nil {
		t.Fatalf("first send failed: %v", err)
	}

	req.RequestID = "req-offline-2"
	second, secondAttempt, err := svc.SendMessage(req)
	if err != nil {
		t.Fatalf("second send failed: %v", err)
	}
	if second.ID != first.ID {
		t.Fatalf("expected compat device to dedupe message")
	}
	if secondAttempt.ID != firstAttempt.ID {
		t.Fatalf("expected compat device to reuse attempt")
	}
}

func TestRetryMessageCreatesNewAttempt(t *testing.T) {
	svc := newService()
	msg, attempt := send(t, svc, "local-retry")
	_, err := svc.CompleteAttempt(service.CompleteAttemptRequest{AttemptID: attempt.ID, Success: false, ErrorCode: "network_error"})
	if err != nil {
		t.Fatalf("complete attempt failed: %v", err)
	}
	retried, retryAttempt, err := svc.RetryMessage(msg.ID)
	if err != nil {
		t.Fatalf("retry failed: %v", err)
	}
	if retryAttempt.ID <= attempt.ID {
		t.Fatalf("expected new attempt")
	}
	if retried.Status != model.MessageStatusSending {
		t.Fatalf("expected sending after retry, got %s", retried.Status)
	}
}

func TestRetryWhileSendingReturnsRunningAttempt(t *testing.T) {
	svc := newService()
	msg, attempt := send(t, svc, "local-retry-running")
	retried, retryAttempt, err := svc.RetryMessage(msg.ID)
	if err != nil {
		t.Fatalf("retry failed: %v", err)
	}
	if retryAttempt.ID != attempt.ID {
		t.Fatalf("expected same running attempt, got first=%d retry=%d", attempt.ID, retryAttempt.ID)
	}
	if retried.Status != model.MessageStatusSending {
		t.Fatalf("expected sending after retry, got %s", retried.Status)
	}
}

func TestRetrySentMessageRejected(t *testing.T) {
	svc := newService()
	msg, attempt := send(t, svc, "local-retry-sent")
	_, err := svc.CompleteAttempt(service.CompleteAttemptRequest{AttemptID: attempt.ID, Success: true})
	if err != nil {
		t.Fatalf("complete attempt failed: %v", err)
	}
	if _, _, err := svc.RetryMessage(msg.ID); err == nil {
		t.Fatalf("expected retrying sent message to fail")
	}
}

func TestCompleteAttemptIgnoresStaleAttemptAfterRetry(t *testing.T) {
	svc := newService()
	msg, attempt := send(t, svc, "local-stale")
	_, err := svc.CompleteAttempt(service.CompleteAttemptRequest{AttemptID: attempt.ID, Success: false, ErrorCode: "network_error"})
	if err != nil {
		t.Fatalf("complete attempt failed: %v", err)
	}
	retried, retryAttempt, err := svc.RetryMessage(msg.ID)
	if err != nil {
		t.Fatalf("retry failed: %v", err)
	}
	if retried.ActiveAttemptID != retryAttempt.ID {
		t.Fatalf("expected retry attempt to become active")
	}

	staleResult, err := svc.CompleteAttempt(service.CompleteAttemptRequest{AttemptID: attempt.ID, Success: true})
	if err != nil {
		t.Fatalf("stale callback failed: %v", err)
	}
	if staleResult.Status != model.MessageStatusSending {
		t.Fatalf("expected stale callback to keep current sending status, got %s", staleResult.Status)
	}

	final, err := svc.CompleteAttempt(service.CompleteAttemptRequest{AttemptID: retryAttempt.ID, Success: true})
	if err != nil {
		t.Fatalf("active callback failed: %v", err)
	}
	if final.Status != model.MessageStatusSent {
		t.Fatalf("expected final sent status, got %s", final.Status)
	}
}

func TestCompleteAttemptReplayIsIdempotent(t *testing.T) {
	svc := newService()
	_, attempt := send(t, svc, "local-replay")
	first, err := svc.CompleteAttempt(service.CompleteAttemptRequest{AttemptID: attempt.ID, Success: true})
	if err != nil {
		t.Fatalf("first callback failed: %v", err)
	}
	second, err := svc.CompleteAttempt(service.CompleteAttemptRequest{AttemptID: attempt.ID, Success: true})
	if err != nil {
		t.Fatalf("replayed callback failed: %v", err)
	}
	if second.Status != model.MessageStatusSent {
		t.Fatalf("expected sent status after replay, got %s", second.Status)
	}
	summary, err := svc.GetConversationSummary(2, 100)
	if err != nil {
		t.Fatalf("summary failed: %v", err)
	}
	if summary.UnreadCount != 1 {
		t.Fatalf("expected replay not to double count unread, got %d", summary.UnreadCount)
	}
	if second.Version != first.Version {
		t.Fatalf("expected replay not to bump version, got first=%d second=%d", first.Version, second.Version)
	}
}

func TestListConversationMessagesAndSummary(t *testing.T) {
	svc := newService()
	for i := 0; i < 3; i++ {
		msg, attempt := send(t, svc, fmt.Sprintf("local-list-%d", i))
		_, err := svc.CompleteAttempt(service.CompleteAttemptRequest{AttemptID: attempt.ID, Success: true})
		if err != nil {
			t.Fatalf("complete attempt failed for msg %d: %v", msg.ID, err)
		}
	}
	items, err := svc.ListConversationMessages(1, 100, 0, 20)
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}
	if len(items) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(items))
	}
	summary, err := svc.GetConversationSummary(2, 100)
	if err != nil {
		t.Fatalf("summary failed: %v", err)
	}
	if summary.UnreadCount == 0 {
		t.Fatalf("expected receiver unread count")
	}
}

func TestSummaryVisibilityAndMarkRead(t *testing.T) {
	svc := newService()
	msg, attempt := send(t, svc, "local-summary-visibility")

	senderSummary, err := svc.GetConversationSummary(1, 100)
	if err != nil {
		t.Fatalf("sender summary failed: %v", err)
	}
	if senderSummary.LastMessageID != msg.ID {
		t.Fatalf("expected sender summary to see sending message, got %d", senderSummary.LastMessageID)
	}

	receiverSummary, err := svc.GetConversationSummary(2, 100)
	if err != nil {
		t.Fatalf("receiver summary failed: %v", err)
	}
	if receiverSummary.LastMessageID != 0 {
		t.Fatalf("expected receiver summary to hide unsent message, got %d", receiverSummary.LastMessageID)
	}

	_, err = svc.CompleteAttempt(service.CompleteAttemptRequest{AttemptID: attempt.ID, Success: true})
	if err != nil {
		t.Fatalf("complete attempt failed: %v", err)
	}

	receiverSummary, err = svc.GetConversationSummary(2, 100)
	if err != nil {
		t.Fatalf("receiver summary failed after sent: %v", err)
	}
	if receiverSummary.LastMessageID != msg.ID {
		t.Fatalf("expected receiver summary to show sent message, got %d", receiverSummary.LastMessageID)
	}
	if receiverSummary.UnreadCount != 1 {
		t.Fatalf("expected unread count 1 after sent message, got %d", receiverSummary.UnreadCount)
	}

	if err := svc.MarkConversationRead(2, 100); err != nil {
		t.Fatalf("mark read failed: %v", err)
	}
	receiverSummary, err = svc.GetConversationSummary(2, 100)
	if err != nil {
		t.Fatalf("receiver summary failed after mark read: %v", err)
	}
	if receiverSummary.UnreadCount != 0 {
		t.Fatalf("expected unread count 0 after mark read, got %d", receiverSummary.UnreadCount)
	}

	_, nextAttempt := send(t, svc, "local-summary-next")
	_, err = svc.CompleteAttempt(service.CompleteAttemptRequest{AttemptID: nextAttempt.ID, Success: true})
	if err != nil {
		t.Fatalf("complete second attempt failed: %v", err)
	}
	receiverSummary, err = svc.GetConversationSummary(2, 100)
	if err != nil {
		t.Fatalf("receiver summary failed after second sent: %v", err)
	}
	if receiverSummary.UnreadCount != 1 {
		t.Fatalf("expected unread count 1 after new sent message, got %d", receiverSummary.UnreadCount)
	}
}

func TestReceiverListHidesUnsentMessages(t *testing.T) {
	svc := newService()
	msg, _ := send(t, svc, "local-visibility-list")

	senderItems, err := svc.ListConversationMessages(1, 100, 0, 20)
	if err != nil {
		t.Fatalf("sender list failed: %v", err)
	}
	if len(senderItems) != 1 || senderItems[0].ID != msg.ID {
		t.Fatalf("expected sender to see sending message in list")
	}

	receiverItems, err := svc.ListConversationMessages(2, 100, 0, 20)
	if err != nil {
		t.Fatalf("receiver list failed: %v", err)
	}
	if len(receiverItems) != 0 {
		t.Fatalf("expected receiver list to hide unsent message, got %d items", len(receiverItems))
	}
}

func TestLegacyDisplayStatusDoesNotChangeSummaryTruth(t *testing.T) {
	svc := newService()
	msg, attempt := send(t, svc, "local-legacy-status")
	_, err := svc.CompleteAttempt(service.CompleteAttemptRequest{AttemptID: attempt.ID, Success: true})
	if err != nil {
		t.Fatalf("complete attempt failed: %v", err)
	}

	receiverSummaryBefore, err := svc.GetConversationSummary(2, 100)
	if err != nil {
		t.Fatalf("summary before legacy status failed: %v", err)
	}
	if receiverSummaryBefore.UnreadCount != 1 {
		t.Fatalf("expected unread count 1 before legacy override, got %d", receiverSummaryBefore.UnreadCount)
	}

	_, err = svc.SetLegacyDisplayStatus(msg.ID, model.MessageStatusFailed)
	if err != nil {
		t.Fatalf("set legacy display status failed: %v", err)
	}

	items, err := svc.ListConversationMessages(1, 100, 0, 20)
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}
	if len(items) == 0 || items[0].Status != model.MessageStatusFailed {
		t.Fatalf("expected legacy display status to affect list rendering")
	}

	stored, err := svc.GetMessage(msg.ID)
	if err != nil {
		t.Fatalf("get message failed: %v", err)
	}
	if stored.Status != model.MessageStatusSent {
		t.Fatalf("expected real message status to remain sent, got %s", stored.Status)
	}

	receiverSummaryAfter, err := svc.GetConversationSummary(2, 100)
	if err != nil {
		t.Fatalf("summary after legacy status failed: %v", err)
	}
	if receiverSummaryAfter.UnreadCount != receiverSummaryBefore.UnreadCount {
		t.Fatalf("expected legacy display status not to change unread truth, before=%d after=%d", receiverSummaryBefore.UnreadCount, receiverSummaryAfter.UnreadCount)
	}
}

func TestSyncReturnsEvents(t *testing.T) {
	svc := newService()
	_, attempt := send(t, svc, "local-sync")
	_, err := svc.CompleteAttempt(service.CompleteAttemptRequest{AttemptID: attempt.ID, Success: true})
	if err != nil {
		t.Fatalf("complete attempt failed: %v", err)
	}
	events, cursor, err := svc.Sync(service.SyncRequest{UserID: 2, DeviceID: "device-b", Cursor: 0})
	if err != nil {
		t.Fatalf("sync failed: %v", err)
	}
	if len(events) == 0 || cursor == 0 {
		t.Fatalf("expected sync events and cursor")
	}
}

func TestSyncCursorRequiresAckBeforeAdvancing(t *testing.T) {
	svc := newService()
	_, attempt := send(t, svc, "local-sync-ack")
	_, err := svc.CompleteAttempt(service.CompleteAttemptRequest{AttemptID: attempt.ID, Success: true})
	if err != nil {
		t.Fatalf("complete attempt failed: %v", err)
	}

	firstEvents, nextCursor, err := svc.Sync(service.SyncRequest{UserID: 2, DeviceID: "device-sync", Cursor: 0, UseAckMode: true})
	if err != nil {
		t.Fatalf("first sync failed: %v", err)
	}
	if len(firstEvents) == 0 || nextCursor == 0 {
		t.Fatalf("expected first sync to return events and cursor")
	}

	secondEvents, secondCursor, err := svc.Sync(service.SyncRequest{UserID: 2, DeviceID: "device-sync", Cursor: 0})
	if err != nil {
		t.Fatalf("second sync failed: %v", err)
	}
	if len(secondEvents) != len(firstEvents) {
		t.Fatalf("expected unacked sync to replay events, got first=%d second=%d", len(firstEvents), len(secondEvents))
	}
	if secondCursor != nextCursor {
		t.Fatalf("expected next cursor to remain stable before ack, got first=%d second=%d", nextCursor, secondCursor)
	}

	ackedEvents, ackedCursor, err := svc.Sync(service.SyncRequest{UserID: 2, DeviceID: "device-sync", AckCursor: nextCursor})
	if err != nil {
		t.Fatalf("ack sync failed: %v", err)
	}
	if len(ackedEvents) != 0 {
		t.Fatalf("expected acked sync to return no new events, got %d", len(ackedEvents))
	}
	if ackedCursor != nextCursor {
		t.Fatalf("expected acked cursor to stay at %d, got %d", nextCursor, ackedCursor)
	}

	postAckEvents, _, err := svc.Sync(service.SyncRequest{UserID: 2, DeviceID: "device-sync", Cursor: 0})
	if err != nil {
		t.Fatalf("post ack sync failed: %v", err)
	}
	if len(postAckEvents) != 0 {
		t.Fatalf("expected no replay after ack, got %d events", len(postAckEvents))
	}

	staleAckEvents, _, err := svc.Sync(service.SyncRequest{UserID: 2, DeviceID: "device-sync", AckCursor: nextCursor - 1})
	if err != nil {
		t.Fatalf("stale ack sync failed: %v", err)
	}
	if len(staleAckEvents) != 0 {
		t.Fatalf("expected stale ack not to rewind cursor, got %d events", len(staleAckEvents))
	}
}

func TestSyncLegacyModeAdvancesCursorWithoutAck(t *testing.T) {
	svc := newService()
	_, attempt := send(t, svc, "local-sync-legacy")
	_, err := svc.CompleteAttempt(service.CompleteAttemptRequest{AttemptID: attempt.ID, Success: true})
	if err != nil {
		t.Fatalf("complete attempt failed: %v", err)
	}

	firstEvents, nextCursor, err := svc.Sync(service.SyncRequest{UserID: 2, DeviceID: "device-legacy-sync", Cursor: 0})
	if err != nil {
		t.Fatalf("first legacy sync failed: %v", err)
	}
	if len(firstEvents) == 0 || nextCursor == 0 {
		t.Fatalf("expected first legacy sync to return events and cursor")
	}

	secondEvents, secondCursor, err := svc.Sync(service.SyncRequest{UserID: 2, DeviceID: "device-legacy-sync", Cursor: 0})
	if err != nil {
		t.Fatalf("second legacy sync failed: %v", err)
	}
	if len(secondEvents) != 0 {
		t.Fatalf("expected legacy sync to advance cursor without ack, got %d replayed events", len(secondEvents))
	}
	if secondCursor != nextCursor {
		t.Fatalf("expected legacy cursor to remain at %d, got %d", nextCursor, secondCursor)
	}
}

func TestMarkReadProducesSummarySyncEvent(t *testing.T) {
	svc := newService()
	_, attempt := send(t, svc, "local-read-sync")
	_, err := svc.CompleteAttempt(service.CompleteAttemptRequest{AttemptID: attempt.ID, Success: true})
	if err != nil {
		t.Fatalf("complete attempt failed: %v", err)
	}

	initialEvents, nextCursor, err := svc.Sync(service.SyncRequest{UserID: 2, DeviceID: "device-read-sync", Cursor: 0})
	if err != nil {
		t.Fatalf("initial sync failed: %v", err)
	}
	if len(initialEvents) == 0 {
		t.Fatalf("expected initial sync events")
	}

	if err := svc.MarkConversationRead(2, 100); err != nil {
		t.Fatalf("mark read failed: %v", err)
	}

	events, _, err := svc.Sync(service.SyncRequest{
		UserID:     2,
		DeviceID:   "device-read-sync",
		Cursor:     0,
		AckCursor:  nextCursor,
		UseAckMode: true,
	})
	if err != nil {
		t.Fatalf("sync after read failed: %v", err)
	}
	found := false
	for _, ev := range events {
		if ev.EventType == model.EventTypeSummaryUpdated {
			found = true
			if ev.SummaryUnreadCount != 0 {
				t.Fatalf("expected summary unread count 0 after mark read, got %d", ev.SummaryUnreadCount)
			}
			if ev.ReadMessageID == 0 {
				t.Fatalf("expected read message id to be populated")
			}
		}
	}
	if !found {
		t.Fatalf("expected mark read to produce summary update sync event")
	}
}

func TestSyncRejectsInvalidRequest(t *testing.T) {
	svc := newService()
	if _, _, err := svc.Sync(service.SyncRequest{UserID: 0, DeviceID: "device"}); err == nil {
		t.Fatalf("expected invalid user id to be rejected")
	}
	if _, _, err := svc.Sync(service.SyncRequest{UserID: 1, DeviceID: "", Cursor: 0}); err == nil {
		t.Fatalf("expected empty device id to be rejected")
	}
	if _, _, err := svc.Sync(service.SyncRequest{UserID: 1, DeviceID: "device", Cursor: -1}); err == nil {
		t.Fatalf("expected negative cursor to be rejected")
	}
}

func TestSyncHTTPRejectsInvalidQuery(t *testing.T) {
	repo := repository.NewMemoryMessageRepository()
	svc := service.NewMessageService(repo)
	server := api.NewServer(svc)
	mux := http.NewServeMux()
	server.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/sync?user_id=nope&device_id=device", nil)
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("expected invalid query to return 400, got %d", resp.Code)
	}
}

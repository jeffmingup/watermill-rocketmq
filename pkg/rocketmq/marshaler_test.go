package rocketmq_test

import (
	"bytes"
	"testing"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/google/uuid"
	"github.com/jeffmingup/watermill-rocketmq/pkg/rocketmq"
)

func TestDefaultMarshaler_Marshal(t *testing.T) {
	t.Parallel()

	marshaler := rocketmq.DefaultMarshaler{}
	msg := message.NewMessage(uuid.New().String(), []byte("test payload"))

	m, err := marshaler.Marshal("test-topic", msg)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if m.Topic != "test-topic" {
		t.Errorf("expected topic to be %q, got %q", "test-topic", m.Topic)
	}

	if !bytes.Equal(m.Body, []byte("test payload")) {
		t.Errorf("expected body to be %v, got %v", []byte("test payload"), m.Body)
	}

	if uuidStr := m.GetProperties()[rocketmq.UUIDHeaderKey]; uuidStr != msg.UUID {
		t.Errorf("expected UUID header to be %q, got %q", msg.UUID, uuidStr)
	}
}

func BenchmarkDefaultMarshaler_Marshal(b *testing.B) {
	marshaler := rocketmq.DefaultMarshaler{}
	msg := message.NewMessage(uuid.New().String(), []byte("test payload"))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		marshaler.Marshal("test-topic", msg) //nolint:errcheck
	}
}

func TestDefaultMarshaler_Unmarshal(t *testing.T) {
	t.Parallel()
	marshaler := rocketmq.DefaultMarshaler{}

	messageID := uuid.New().String()
	body := []byte("test payload")
	properties := map[string]string{
		"foo":                  "bar",
		rocketmq.UUIDHeaderKey: messageID,
	}
	msg := primitive.NewMessage("test-topic", body)
	msg.WithProperties(properties)

	unmarshaledMsg, err := marshaler.Unmarshal(msg)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if unmarshaledMsg.UUID != messageID {
		t.Errorf("expected UUID to be %q, got %q", messageID, unmarshaledMsg.UUID)
	}

	if !bytes.Equal(unmarshaledMsg.Payload, body) {
		t.Errorf("expected payload to be %v, got %v", body, unmarshaledMsg.Payload)
	}

	if unmarshaledMsg.Metadata["foo"] != "bar" {
		t.Errorf("expected metadata to contain key 'foo' with value 'bar', got %v", unmarshaledMsg.Metadata)
	}
}

func BenchmarkDefaultMarshaler_Unmarshal(b *testing.B) {
	marshaler := rocketmq.DefaultMarshaler{}
	messageID := uuid.New().String()
	body := []byte("test payload")
	properties := map[string]string{
		"foo":                  "bar",
		rocketmq.UUIDHeaderKey: messageID,
	}
	msg := primitive.NewMessage("test-topic", body)
	msg.WithProperties(properties)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		marshaler.Unmarshal(msg) //nolint:errcheck
	}
}

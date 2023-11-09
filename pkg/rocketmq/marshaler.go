package rocketmq

import (
	"bytes"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

const (
	UUIDHeaderKey = "_watermill_message_uuid"
	placeholder   = "#placeholder"
)

var placeholderBytes = []byte(placeholder)

// Marshaler marshals Watermill's message to rocketMQ message.
type Marshaler interface {
	Marshal(topic string, msg *message.Message) (*primitive.Message, error)
}

// Unmarshaler unmarshals rocketMQ's message to Watermill's message.
type Unmarshaler interface {
	Unmarshal(*primitive.Message) (*message.Message, error)
}

type MarshalerUnmarshaler interface {
	Marshaler
	Unmarshaler
}

var _ MarshalerUnmarshaler = &DefaultMarshaler{}

type DefaultMarshaler struct{}

func (DefaultMarshaler) Marshal(topic string, msg *message.Message) (*primitive.Message, error) {
	if msg.Payload == nil {
		msg.Payload = placeholderBytes
	}
	m := &primitive.Message{
		Topic: topic,
		Body:  msg.Payload,
	}
	property := msg.Metadata
	if msg.UUID != "" {
		property[UUIDHeaderKey] = msg.UUID
	}

	m.WithProperties(property)

	return m, nil
}

func (DefaultMarshaler) Unmarshal(rocketmqMsg *primitive.Message) (*message.Message, error) {
	var messageID string
	metadata := make(message.Metadata, len(rocketmqMsg.GetProperties()))

	for k, v := range rocketmqMsg.GetProperties() {
		if k == UUIDHeaderKey {
			messageID = v
		} else {
			metadata.Set(k, v)
		}
	}
	if bytes.Equal(rocketmqMsg.Body, placeholderBytes) {
		rocketmqMsg.Body = nil
	}
	msg := message.NewMessage(messageID, rocketmqMsg.Body)
	msg.Metadata = metadata

	return msg, nil
}

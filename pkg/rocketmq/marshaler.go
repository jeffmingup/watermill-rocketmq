package rocketmq

import (
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/pkg/errors"
)

const UUIDHeaderKey = "_watermill_message_uuid"

// Marshaler marshals Watermill's message to rocketMQ message.
type Marshaler interface {
	Marshal(topic string, msg *message.Message) (*primitive.Message, error)
}

// Unmarshaler unmarshals rocketMQ's message to Watermill's message.
type Unmarshaler interface {
	Unmarshal(*primitive.MessageExt) (*message.Message, error)
}

type MarshalerUnmarshaler interface {
	Marshaler
	Unmarshaler
}

var _ MarshalerUnmarshaler = &DefaultMarshaler{}

type DefaultMarshaler struct{}

func (DefaultMarshaler) Marshal(topic string, msg *message.Message) (*primitive.Message, error) {
	if value := msg.Metadata.Get(UUIDHeaderKey); value != "" {
		return nil, errors.Errorf("metadata %s is reserved by watermill for message UUID", UUIDHeaderKey)
	}
	m := &primitive.Message{
		Topic: topic,
		Body:  msg.Payload,
	}
	m.WithProperty(UUIDHeaderKey, msg.UUID)
	m.WithProperties(msg.Metadata)

	return m, nil
}

func (DefaultMarshaler) Unmarshal(rocketmqMsg *primitive.MessageExt) (*message.Message, error) {
	var messageID string
	metadata := make(message.Metadata, len(rocketmqMsg.GetProperties()))

	for k, v := range rocketmqMsg.GetProperties() {
		if k == UUIDHeaderKey {
			messageID = v
		} else {
			metadata.Set(k, v)
		}
	}

	msg := message.NewMessage(messageID, rocketmqMsg.Body)
	msg.Metadata = metadata

	return msg, nil
}

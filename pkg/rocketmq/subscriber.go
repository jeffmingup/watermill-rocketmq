package rocketmq

import (
	"context"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/pkg/errors"
)

type Subscriber struct {
	config   SubscriberConfig
	logger   watermill.LoggerAdapter
	consumer rocketmq.PushConsumer

	closed bool
}
type SubscriberConfig struct {
	// rocketMQ brokers list.
	Brokers []string

	// Unmarshaler is used to unmarshal messages from rocketMQ format into Watermill format.
	Unmarshaler Unmarshaler

	// rocketMQ consumer group.
	// When empty, all messages from all partitions will be returned.
	ConsumerGroup string

	// If true then each consumed message will be wrapped with Opentelemetry tracing, provided by otelsarama.
	//
	// Deprecated: pass OTELSaramaTracer to Tracer field instead.
	OTELEnabled bool
}

func NewSubscriber(logger watermill.LoggerAdapter) (*Subscriber, error) {
	c, err := rocketmq.NewPushConsumer(
		consumer.WithGroupName("testGroup"),
		consumer.WithNsResolver(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"})),
		consumer.WithConsumerOrder(true),
	)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create rocketMQ consumer")
	}
	return &Subscriber{
		logger: logger,
		config: SubscriberConfig{
			Unmarshaler: DefaultMarshaler{},
		},
		consumer: c,
	}, nil
}

func (s *Subscriber) Subscribe(topic string) (<-chan *message.Message, error) {
	s.logger.Debug("Subscribing to topic", watermill.LogFields{"topic": topic})

	output := make(chan *message.Message)
	err := s.consumer.Subscribe(topic, consumer.MessageSelector{}, func(ctx context.Context,
		msgs ...*primitive.MessageExt,
	) (consumer.ConsumeResult, error) {
		for i := range msgs {
			receivedMsgLogFields := watermill.LogFields{
				"OffsetMsgId": msgs[i].OffsetMsgId,
				"QueueOffset": msgs[i].QueueOffset,
				"Queue":       msgs[i].Queue.String(),
			}
			// s.logger.Trace("Received message from rocketMQ", receivedMsgLogFields)
			msg, err := s.config.Unmarshaler.Unmarshal(msgs[i])
			if err != nil {
				return consumer.SuspendCurrentQueueAMoment, errors.Wrap(err, "message unmarshal failed")
			}

			msg.SetContext(ctx)

			receivedMsgLogFields = receivedMsgLogFields.Add(watermill.LogFields{
				"message_uuid": msg.UUID,
			})
			select {
			case output <- msg:
				s.logger.Trace("Message sent to consumer", receivedMsgLogFields)
			case <-ctx.Done():
				s.logger.Trace("Closing, ctx cancelled before sent to consumer", receivedMsgLogFields)
				return consumer.SuspendCurrentQueueAMoment, nil
			}

			select {
			case <-msg.Acked():
				s.logger.Trace("Message Acked", receivedMsgLogFields)
			case <-msg.Nacked():
				s.logger.Trace("Message Nacked", receivedMsgLogFields)
				return consumer.SuspendCurrentQueueAMoment, nil
			case <-ctx.Done():
				s.logger.Trace("Closing, ctx cancelled before ack", receivedMsgLogFields)
				return consumer.SuspendCurrentQueueAMoment, nil
			}

		}

		return consumer.ConsumeSuccess, nil
	})
	if err != nil {
		return nil, err
	}
	err = s.consumer.Start()
	if err != nil {
		return nil, err
	}
	return output, nil
}

func (s *Subscriber) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true

	if err := s.consumer.Shutdown(); err != nil {
		return errors.Wrap(err, "cannot close rocketMQ consumer")
	}

	return nil
}

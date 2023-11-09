package rocketmq

import (
	"context"
	"sync"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/pkg/errors"
)

type SubscriberConfig struct {
	// Unmarshaler is used to unmarshal messages from rocketMQ format into Watermill format.
	Unmarshaler Unmarshaler

	Option []consumer.Option
}

func DefaultSubscriberConfig(consumerGroup string, addr ...string) *SubscriberConfig {
	return &SubscriberConfig{
		Unmarshaler: DefaultMarshaler{},
		Option: []consumer.Option{
			consumer.WithGroupName(consumerGroup),
			consumer.WithNsResolver(primitive.NewPassthroughResolver(addr)),
		},
	}
}

type Subscriber struct {
	config        *SubscriberConfig
	logger        watermill.LoggerAdapter
	closed        bool
	closing       chan struct{}
	subscribersWg sync.WaitGroup
	pushConsumer  rocketmq.PushConsumer
}

func NewSubscriber(config *SubscriberConfig, logger watermill.LoggerAdapter) (*Subscriber, error) {
	if logger == nil {
		logger = watermill.NopLogger{}
	}
	pushConsumer, err := rocketmq.NewPushConsumer(config.Option...)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create rocketMQ consumer")
	}
	return &Subscriber{
		logger:       logger,
		config:       config,
		closed:       true,
		closing:      make(chan struct{}),
		pushConsumer: pushConsumer,
	}, nil
}

func (s *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	s.subscribersWg.Add(1)
	logFields := watermill.LogFields{
		"provider":            "rocketMQ",
		"topic":               topic,
		"kafka_consumer_uuid": watermill.NewShortUUID(),
	}
	s.logger.Info("Subscribing to Kafka topic", logFields)
	output := make(chan *message.Message)
	once := &sync.Once{}

	ctx, cancel := context.WithCancel(ctx)
	err := s.pushConsumer.Subscribe(topic, consumer.MessageSelector{}, s.consumeMessages(ctx, output, once, topic))
	if err != nil {
		s.subscribersWg.Done()
		cancel()
		return nil, err
	}
	err = s.pushConsumer.Start()
	if err != nil {
		s.subscribersWg.Done()
		cancel()
		return nil, err
	}
	s.closed = false // mark as open
	go func() {
		<-s.closing
		if err := s.pushConsumer.Shutdown(); err != nil {
			s.logger.Error("cannot close rocketMQ consumer", err, logFields)
		}
		s.logger.Trace("Closing subscriber, cancelling consumeMessages", logFields)
		once.Do(func() {
			close(output)
		})
		s.subscribersWg.Done()
		cancel()
	}()

	return output, nil
}

func (s *Subscriber) consumeMessages(
	ctx context.Context, output chan *message.Message, once *sync.Once, topic string) func(
	context.Context, ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
	return func(_ context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for _, m := range msgs {
			select {
			case <-ctx.Done():
				once.Do(func() {
					close(output)
					if err := s.pushConsumer.Unsubscribe(topic); err != nil {
						s.logger.Error("cannot Unsubscribe", err, watermill.LogFields{"topic": topic})
					}
				})
				return consumer.SuspendCurrentQueueAMoment, nil
			case <-s.closing:
				return consumer.SuspendCurrentQueueAMoment, nil
			default:
				result, err := s.precessMsg(ctx, m, output, once, topic)
				if err != nil {
					return result, err
				}
				if result != consumer.ConsumeSuccess {
					return result, err
				}
			}
		}
		return consumer.ConsumeSuccess, nil
	}
}

func (s *Subscriber) precessMsg(
	ctx context.Context, m *primitive.MessageExt, output chan *message.Message, once *sync.Once, topic string) (
	consumer.ConsumeResult, error,
) {
	receivedMsgLogFields := watermill.LogFields{
		"OffsetMsgId": m.OffsetMsgId,
		"QueueOffset": m.QueueOffset,
		"Queue":       m.Queue.String(),
		"Payload":     string(m.Body),
	}
	msg, err := s.config.Unmarshaler.Unmarshal(&m.Message)
	if err != nil {
		return consumer.SuspendCurrentQueueAMoment, errors.Wrap(err, "message unmarshal failed")
	}
	ctx, cancelCtx := context.WithCancel(ctx)
	msg.SetContext(ctx)
	defer cancelCtx()
	receivedMsgLogFields = receivedMsgLogFields.Add(watermill.LogFields{
		"message_uuid": msg.UUID,
	})
	select {
	case output <- msg:
		s.logger.Trace("Message sent to consumer", receivedMsgLogFields)
	case <-ctx.Done():
		s.logger.Trace("Closing, ctx cancelled before sent to consumer", receivedMsgLogFields)

		once.Do(func() {
			close(output)
			if err := s.pushConsumer.Unsubscribe(topic); err != nil {
				s.logger.Error("cannot Unsubscribe", err, watermill.LogFields{"topic": topic})
			}
		})
		return consumer.SuspendCurrentQueueAMoment, nil
	case <-s.closing:
		s.logger.Trace("Closing, s.closing before sent to consumer", receivedMsgLogFields)
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

		once.Do(func() {
			close(output)
			if err := s.pushConsumer.Unsubscribe(topic); err != nil {
				s.logger.Error("cannot Unsubscribe", err, watermill.LogFields{"topic": topic})
			}
		})
		return consumer.SuspendCurrentQueueAMoment, nil
	case <-s.closing:
		s.logger.Trace("Closing, s.closing before sent to consumer", receivedMsgLogFields)
		return consumer.SuspendCurrentQueueAMoment, nil
	}
	return consumer.ConsumeSuccess, nil
}

func (s *Subscriber) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true

	close(s.closing)
	s.subscribersWg.Wait()

	s.logger.Debug("rocketMQ subscriber closed", nil)
	return nil
}

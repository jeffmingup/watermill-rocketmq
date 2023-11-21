package rocketmq

import (
	"context"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/admin"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/pkg/errors"
	"sync"
)

type SubscriberConfig struct {
	// Unmarshaler is used to unmarshal messages from rocketMQ format into Watermill format.
	Unmarshaler            Unmarshaler
	Addr                   []string
	Option                 []consumer.Option
	consumerGroup          string
	InitializeTopicOptions []admin.OptionCreate
}

func DefaultSubscriberConfig(consumerGroup string, addr ...string) SubscriberConfig {
	return SubscriberConfig{
		consumerGroup: consumerGroup,
		Addr:          addr,
		Unmarshaler:   DefaultMarshaler{},
		Option: []consumer.Option{
			consumer.WithNsResolver(primitive.NewPassthroughResolver(addr)),
			consumer.WithMaxReconsumeTimes(2),
			consumer.WithConsumerOrder(true),
		},
	}
}

type Subscriber struct {
	config        SubscriberConfig
	logger        watermill.LoggerAdapter
	closed        bool
	closing       chan struct{}
	subscribersWg sync.WaitGroup
}

func NewSubscriber(config SubscriberConfig, logger watermill.LoggerAdapter) (*Subscriber, error) {
	if logger == nil {
		logger = watermill.NopLogger{}
	}

	return &Subscriber{
		logger:  logger,
		config:  config,
		closed:  true,
		closing: make(chan struct{}),
	}, nil
}

func (s *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	logFields := watermill.LogFields{
		"provider":      "rocketMQ subscribe",
		"topic":         topic,
		"consumerGroup": s.config.consumerGroup,
	}
	s.logger.Info("Subscribing to rocketMQ topic", logFields)

	option := make([]consumer.Option, len(s.config.Option))
	copy(option, s.config.Option)

	option = append(option,
		consumer.WithInstance(watermill.NewUUID()),
		// 避免同一个消费者订阅不同的topic,导致订阅关系不一致
		// https://rocketmq.apache.org/zh/docs/bestPractice/05subscribe#31-%E5%90%8C%E4%B8%80consumergroup%E4%B8%8B%E7%9A%84consumer%E5%AE%9E%E4%BE%8B%E8%AE%A2%E9%98%85%E7%9A%84topic%E4%B8%8D%E5%90%8C3x4x-sdk%E9%80%82%E7%94%A8
		consumer.WithGroupName(s.config.consumerGroup+"_"+topic))

	pushConsumer, err := rocketmq.NewPushConsumer(
		option...)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create rocketMQ consumer")
	}
	output := make(chan *message.Message)
	if err := pushConsumer.Subscribe(topic, consumer.MessageSelector{}, func(_ context.Context,
		msgs ...*primitive.MessageExt,
	) (consumer.ConsumeResult, error) {

		for _, msg := range msgs {
			select {
			case <-ctx.Done():
				return consumer.SuspendCurrentQueueAMoment, nil
			case <-s.closing:
				return consumer.SuspendCurrentQueueAMoment, nil
			default:

			}

			err := s.precess(ctx, msg, output)
			if err != nil {
				//if errors.Is(err, ErrSubscriberClose) || errors.Is(err, ErrCtxDone) {
				//    return consumer.SuspendCurrentQueueAMoment, nil
				//}
				return consumer.SuspendCurrentQueueAMoment, nil
			}

		}
		return consumer.ConsumeSuccess, nil
	}); err != nil {
		return nil, err
	}
	if err := pushConsumer.Start(); err != nil {
		s.logger.Error("pushConsumer.Start error", err, logFields)
	}

	s.subscribersWg.Add(1)
	go func() {
		defer func() {
			err := pushConsumer.Shutdown()
			if err != nil {
				s.logger.Error("pushConsumer Shutdown error", err, logFields)
			}
			s.logger.Info("Closing subscriber, cancelling consumeMessages", logFields)
			close(output)
			s.subscribersWg.Done()
		}()
		select {
		case <-s.closing:
			s.logger.Info("s.closing", logFields)
		case <-ctx.Done():
			s.logger.Info("ctx.Done()", logFields)
		}

	}()
	s.closed = false
	return output, nil
}

var ErrMsgNackd = errors.New("Message Nacked")
var ErrSubscriberClose = errors.New("Subscriber Closing")
var ErrCtxDone = errors.New("ctx.Done")

func (s *Subscriber) precess(
	ctx context.Context, m *primitive.MessageExt, output chan *message.Message) error {

	receivedMsgLogFields := watermill.LogFields{
		"msg": m,
	}
	msg, err := s.config.Unmarshaler.Unmarshal(&m.Message)
	if err != nil {
		return errors.Wrap(err, "message unmarshal failed")
	}

	receivedMsgLogFields = receivedMsgLogFields.Add(watermill.LogFields{
		"message_uuid": msg.UUID,
	})
	msgCtx, cancelCtx := context.WithCancel(ctx)
	defer cancelCtx()
precessLoop:
	for {
		msgToSend := msg.Copy()
		msgToSend.SetContext(msgCtx)
		select {
		case output <- msgToSend:
			s.logger.Trace("Message sent to consumer", receivedMsgLogFields)
		case <-ctx.Done():
			s.logger.Info("Closing, ctx cancelled before sent to consumer", receivedMsgLogFields)
			return ErrCtxDone
		case <-s.closing:
			s.logger.Info("Closing, message discarded", receivedMsgLogFields)
			return ErrSubscriberClose
		}

		select {
		case <-msgToSend.Acked():
			s.logger.Trace("Message Acked", receivedMsgLogFields)
			break precessLoop
		case <-msgToSend.Nacked():
			s.logger.Info("Message Nacked", receivedMsgLogFields)
			return ErrMsgNackd
		case <-ctx.Done():
			s.logger.Info("Closing, ctx cancelled before ack", receivedMsgLogFields)
			return ErrCtxDone
		case <-s.closing:
			s.logger.Info("Closing, message discarded before ack", receivedMsgLogFields)
			return ErrSubscriberClose
		}
	}
	return nil
}

func (s *Subscriber) Close() error {
	if s.closed {
		return nil
	}
	s.logger.Info("Closing subscriber", nil)
	s.closed = true

	close(s.closing)
	s.subscribersWg.Wait()

	return nil
}

// InitializeLock Concurrent creation of topic may cause timeout
var InitializeLock sync.Mutex //

func (s *Subscriber) SubscribeInitialize(topic string) (err error) {
	if len(s.config.InitializeTopicOptions) == 0 {
		return nil
	}
	InitializeLock.Lock()
	defer InitializeLock.Unlock()

	rocketMQAdmin, err := admin.NewAdmin(admin.WithResolver(primitive.NewPassthroughResolver(s.config.Addr)))
	if err != nil {
		s.logger.Error("NewAdmin error ", err, watermill.LogFields{"Addr": s.config.Addr})
		return err
	}
	option := make([]admin.OptionCreate, len(s.config.InitializeTopicOptions))
	copy(option, s.config.InitializeTopicOptions)
	option = append(option, admin.WithTopicCreate(topic))

	if err := rocketMQAdmin.CreateTopic(context.Background(), option...); err != nil {
		s.logger.Error("CreateTopic error ", err, watermill.LogFields{"topic": topic})
		return err
	}
	s.logger.Trace("CreateTopic success ", watermill.LogFields{"topic": topic})
	err = rocketMQAdmin.Close()
	if err != nil {
		s.logger.Error("rocketMQAdmin Close error ", err, watermill.LogFields{"topic": topic})
		return err
	}
	return nil
}

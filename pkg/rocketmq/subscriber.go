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
	"time"
)

type SubscriberConfig struct {
	// Unmarshaler is used to unmarshal messages from rocketMQ format into Watermill format.
	Unmarshaler   Unmarshaler
	Addr          []string
	Option        []consumer.Option
	consumerGroup string
}

func DefaultSubscriberConfig(consumerGroup string, addr ...string) *SubscriberConfig {
	return &SubscriberConfig{
		consumerGroup: consumerGroup,
		Addr:          addr,
		Unmarshaler:   DefaultMarshaler{},
		Option: []consumer.Option{
			consumer.WithGroupName(consumerGroup),
			consumer.WithNsResolver(primitive.NewPassthroughResolver(addr)),
			consumer.WithMaxReconsumeTimes(2),
		},
	}
}

type Subscriber struct {
	config        *SubscriberConfig
	logger        watermill.LoggerAdapter
	closed        bool
	closing       chan struct{}
	subscribersWg sync.WaitGroup
	pullConsumer  rocketmq.PullConsumer
}

func NewSubscriber(config *SubscriberConfig, logger watermill.LoggerAdapter) (*Subscriber, error) {
	if logger == nil {
		logger = watermill.NopLogger{}
	}
	pullConsumer, err := rocketmq.NewPullConsumer(
		config.Option...)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create rocketMQ consumer")
	}
	return &Subscriber{
		logger:       logger,
		config:       config,
		closed:       true,
		closing:      make(chan struct{}),
		pullConsumer: pullConsumer,
	}, nil
}

func (s *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	s.subscribersWg.Add(1)
	logFields := watermill.LogFields{
		"provider":      "rocketMQ poll",
		"topic":         topic,
		"consumerGroup": s.config.consumerGroup,
	}
	s.logger.Info("Subscribing to rocketMQ topic", logFields)
	output := make(chan *message.Message)

	if err := s.pullConsumer.Subscribe(topic, consumer.MessageSelector{}); err != nil {
		return nil, err
	}
	if err := s.pullConsumer.Start(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		if err := s.Poll(ctx, output, logFields); err != nil {
			s.logger.Error(" s.Poll Error", err, logFields)
		}
		close(output)
	}()

	if err := s.pullConsumer.Start(); err != nil {
		s.subscribersWg.Done()
		close(output)
		cancel()
		return nil, err
	}
	s.closed = false
	go func() {
		<-s.closing
		cancel()
		if err := s.pullConsumer.Shutdown(); err != nil {
			s.logger.Error("cannot close rocketMQ consumer", err, logFields)
		}
		s.logger.Trace("Closing subscriber, cancelling consumeMessages", logFields)
		s.subscribersWg.Done()

	}()

	return output, nil
}
func (s *Subscriber) Poll(ctx context.Context, output chan *message.Message, logFields watermill.LogFields) error {
pollLoop:
	for {
		select {
		case <-ctx.Done():
			s.logger.Info("ctx.Done(), before Poll", logFields)
			return nil
		case <-s.closing:
			s.logger.Info("s.closing,  before Poll", logFields)
			return nil
		default:
			cr, err := s.pullConsumer.Poll(ctx, time.Second*5)
			if consumer.IsNoNewMsgError(err) {
				s.logger.Info("IsNoNewMsg：未拉取到消息", logFields)
				continue
			}
			if err != nil {
				s.logger.Error("poll error", err, logFields)
				return err
			}
			s.logger.Trace("poll success", logFields.Add(watermill.LogFields{"cr": cr}))

			for _, m := range cr.GetMsgList() {
				err := s.precess(ctx, m, output)
				if err != nil {
					s.pullConsumer.ACK(context.TODO(), cr, consumer.ConsumeRetryLater)
					if errors.Is(err, ErrMsgNackd) {
						continue pollLoop // 继续拉取
					} else {
						return err
					}
				}

			}
			s.pullConsumer.ACK(context.TODO(), cr, consumer.ConsumeSuccess)
		}

	}
}

var ErrMsgNackd = errors.New("Message Nacked")

func (s *Subscriber) precess(
	ctx context.Context, m *primitive.MessageExt, output chan *message.Message) error {
	receivedMsgLogFields := watermill.LogFields{
		"OffsetMsgId": m.OffsetMsgId,
		"QueueOffset": m.QueueOffset,
		"Queue":       m.Queue.String(),
		"Payload":     string(m.Body),
	}
	msg, err := s.config.Unmarshaler.Unmarshal(&m.Message)
	if err != nil {
		return errors.Wrap(err, "message unmarshal failed")
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
		s.logger.Info(" ctx cancelled before sent to consumer", receivedMsgLogFields)
		return errors.New(" ctx cancelled before sent to consumer")
	case <-s.closing:
		s.logger.Info("s.closing before sent to consumer", receivedMsgLogFields)
		return errors.New("s.closing before sent to consumer")
	}

	select {
	case <-msg.Acked():
		s.logger.Trace("Message Acked", receivedMsgLogFields)

	case <-msg.Nacked():
		s.logger.Info("Message Nacked", receivedMsgLogFields)
		return ErrMsgNackd
	case <-ctx.Done():
		s.logger.Info("Closing, ctx cancelled before ack", receivedMsgLogFields)
		return errors.New("Closing, ctx cancelled before ack")
	case <-s.closing:
		s.logger.Info("Closing, s.closing before sent to consumer", receivedMsgLogFields)
		return errors.New("Closing, s.closing before sent to consumer")
	}
	return nil
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

func (s *Subscriber) SubscribeInitialize(topic string) (err error) {
	rocketMQAdmin, err := admin.NewAdmin(admin.WithResolver(primitive.NewPassthroughResolver(s.config.Addr)))
	if err != nil {
		return err
	}
	topicList, err := rocketMQAdmin.FetchAllTopicList(context.Background())
	if err != nil {
		return err
	}
	for _, t := range topicList.TopicList {
		if t == topic {
			s.logger.Debug("topic 已存在无需创建"+topic, nil)
			return nil
		}
	}
	if err := rocketMQAdmin.CreateTopic(context.Background(), admin.WithTopicCreate(topic), admin.WithBrokerAddrCreate("192.168.144.47:10911")); err != nil {
		return err
	}
	s.logger.Debug("CreateTopic ok "+topic, nil)
	err = rocketMQAdmin.Close()
	if err != nil {
		return err
	}
	return nil
}

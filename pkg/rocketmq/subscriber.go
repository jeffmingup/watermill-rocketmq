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
	Unmarshaler    Unmarshaler
	Addr           []string
	BrokerAddr     string // SubscribeInitialize create topic need brokerAddr，
	Option         []consumer.Option
	consumerGroup  string
	ConsumeOrderly bool
	PollTimeout    time.Duration
}

func DefaultSubscriberConfig(consumerGroup string, addr ...string) SubscriberConfig {
	return SubscriberConfig{
		consumerGroup: consumerGroup,
		Addr:          addr,
		Unmarshaler:   DefaultMarshaler{},
		Option: []consumer.Option{
			consumer.WithNsResolver(primitive.NewPassthroughResolver(addr)),
			consumer.WithMaxReconsumeTimes(2),
		},
		PollTimeout: time.Second * 3,
	}
}

type Subscriber struct {
	config         SubscriberConfig
	logger         watermill.LoggerAdapter
	closed         bool
	closing        chan struct{}
	subscribersWg  sync.WaitGroup
	InitializeLock sync.Mutex
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
		"provider":      "rocketMQ poll",
		"topic":         topic,
		"consumerGroup": s.config.consumerGroup,
	}
	s.logger.Info("Subscribing to rocketMQ topic", logFields)

	option := make([]consumer.Option, len(s.config.Option))
	copy(option, s.config.Option)
	option = append(option,
		consumer.WithInstance(watermill.NewUUID()),
		consumer.WithGroupName(s.config.consumerGroup+"_"+topic))

	pullConsumer, err := rocketmq.NewPullConsumer(
		option...)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create rocketMQ consumer")
	}
	output := make(chan *message.Message)
	if err := pullConsumer.Subscribe(topic, consumer.MessageSelector{}); err != nil {
		return nil, err
	}
	if err := pullConsumer.Start(); err != nil {
		s.logger.Error("pullConsumer.Start error", err, logFields)
	}

	s.subscribersWg.Add(1)
	go func() {
		defer func() {
			close(output)
			err := pullConsumer.Shutdown()
			if err != nil {
				s.logger.Error("Closing subscriber, cancelling consumeMessages", err, logFields)
			}
			s.logger.Info("Closing subscriber, cancelling consumeMessages", logFields)
			s.subscribersWg.Done()
		}()
		if err := s.Poll(ctx, output, pullConsumer, logFields); err != nil {
			s.logger.Error(" s.Poll Error", err, logFields)
		}
	}()
	s.closed = false
	return output, nil
}
func (s *Subscriber) Poll(ctx context.Context, output chan *message.Message, pullConsumer rocketmq.PullConsumer, logFields watermill.LogFields) error {
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
			cr, err := pullConsumer.Poll(ctx, s.config.PollTimeout)
			if consumer.IsNoNewMsgError(err) {
				s.logger.Trace("IsNoNewMsg：未拉取到消息", logFields.Add(watermill.LogFields{"cr": cr}))
				continue
			}
			if err != nil {
				s.logger.Error("poll error", err, logFields)
				return err
			}
			s.logger.Trace("poll success", logFields.Add(watermill.LogFields{"cr": cr, "len(msg)": len(cr.GetMsgList()), "msg": cr.GetMsgList()[0].Message.GetProperty("_watermill_message_uuid"), "GetMQ": cr.GetMQ(), "GetPQ": cr.GetPQ()}))
			for _, m := range cr.GetMsgList() {
				err := s.precess(ctx, m, output)
				if err != nil {
					if !s.config.ConsumeOrderly {
						pullConsumer.ACK(context.TODO(), cr, consumer.ConsumeRetryLater)
					}

					if errors.Is(err, ErrMsgNackd) {
						continue pollLoop // 继续拉取
					} else {
						return err
					}
				}

			}
			pullConsumer.ACK(context.TODO(), cr, consumer.ConsumeSuccess)
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

	receivedMsgLogFields = receivedMsgLogFields.Add(watermill.LogFields{
		"message_uuid": msg.UUID,
	})
precessLoop:
	for {
		msgCtx, cancelCtx := context.WithCancel(ctx)
		msg.SetContext(msgCtx)
		defer cancelCtx()
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
			break precessLoop
		case <-msg.Nacked():
			s.logger.Info("Message Nacked", receivedMsgLogFields)
			if s.config.ConsumeOrderly {
				msg = msg.Copy()
				continue precessLoop // 顺序消费，不可以跳过消息
			}
			return ErrMsgNackd
		case <-ctx.Done():
			s.logger.Info("Closing, ctx cancelled before ack", receivedMsgLogFields)
			return errors.New("Closing, ctx cancelled before ack")
		case <-s.closing:
			s.logger.Info("Closing, s.closing before sent to consumer", receivedMsgLogFields)
			return errors.New("Closing, s.closing before sent to consumer")
		}
	}
	return nil
}

func (s *Subscriber) Close() error {
	if s.closed {
		return nil
	}
	s.logger.Info("Closing subscriber.", nil)
	s.closed = true

	close(s.closing)
	s.subscribersWg.Wait()

	return nil
}

var InitializeLock sync.Mutex

func (s *Subscriber) SubscribeInitialize(topic string) (err error) {
	InitializeLock.Lock()
	defer InitializeLock.Unlock()
	if s.config.BrokerAddr == "" {
		return nil
	}
	rocketMQAdmin, err := admin.NewAdmin(admin.WithResolver(primitive.NewPassthroughResolver(s.config.Addr)))
	if err != nil {
		return err
	}
	if err := rocketMQAdmin.CreateTopic(context.Background(), admin.WithTopicCreate(topic), admin.WithBrokerAddrCreate(s.config.BrokerAddr)); err != nil {
		return err
	}
	s.logger.Debug("CreateTopic ok "+topic, nil)
	err = rocketMQAdmin.Close()
	if err != nil {
		return err
	}
	return nil
}

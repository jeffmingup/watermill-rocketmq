package rocketmq

import (
	"context"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type Publisher struct {
	config   PublisherConfig
	producer rocketmq.Producer
	logger   watermill.LoggerAdapter

	closed bool
}

type PublisherConfig struct {
	Option []producer.Option
	// Marshaler is used to marshal messages from Watermill format into rocketmq format.
	Marshaler Marshaler
}

// NewPublisher creates a new rocketmq Publisher.
func NewPublisher(
	config PublisherConfig,
	logger watermill.LoggerAdapter,
) (*Publisher, error) {
	if logger == nil {
		logger = watermill.NopLogger{}
	}
	p, err := rocketmq.NewProducer(config.Option...)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create rocketMQ producer")
	}
	err = p.Start()
	if err != nil {
		return nil, errors.Wrap(err, "rocketMQ producer start err")
	}
	return &Publisher{
		config:   config,
		producer: p,
		logger:   logger,
	}, nil
}

func DefaultPublisherConfig(addr ...string) PublisherConfig {
	return PublisherConfig{
		Option: []producer.Option{
			producer.WithNsResolver(primitive.NewPassthroughResolver(addr)),
			producer.WithRetry(2),
			producer.WithQueueSelector(producer.NewHashQueueSelector()),
		},
		Marshaler: DefaultMarshaler{},
	}
}

// Publish publishes message to rocketMQ.
//
// Publish is blocking and wait for ack from rocketMQ.
// When one of messages delivery fails - function is interrupted.
func (p *Publisher) Publish(topic string, msgs ...*message.Message) error {
	if p.closed {
		return errors.New("publisher closed")
	}

	logFields := make(watermill.LogFields, 3)
	logFields["topic"] = topic

	for _, msg := range msgs {
		logFields["message_uuid"] = msg.UUID
		p.logger.Trace("Sending message to Kafka", logFields)

		rockerMQMsg, err := p.config.Marshaler.Marshal(topic, msg)
		if err != nil {
			return errors.Wrapf(err, "cannot marshal message %s", msg.UUID)
		}

		res, err := p.producer.SendSync(context.Background(), rockerMQMsg)
		if err != nil {
			return errors.Wrapf(err, "cannot produce message %s", msg.UUID)
		}

		logFields["rocketmq_send_result"] = res

		p.logger.Trace("Message sent to Kafka", logFields)
	}

	return nil
}

func (p *Publisher) Close() error {
	if p.closed {
		return nil
	}
	p.closed = true

	if err := p.producer.Shutdown(); err != nil {
		return errors.Wrap(err, "cannot close rocketMQ producer")
	}

	return nil
}

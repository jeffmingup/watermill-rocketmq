package rocketmq_test

import (
	"os"
	"strings"
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/apache/rocketmq-client-go/v2/rlog"
	"github.com/jeffmingup/watermill-rocketmq/pkg/rocketmq"
)

func roctetMQBrokers() []string {
	brokers := os.Getenv("WATERMILL_TEST_ROCTETMQ_BROKERS")
	if brokers != "" {
		return strings.Split(brokers, ",")
	}
	return []string{"127.0.0.1:9876"}
}

func TestPublishSubscribe(t *testing.T) {
	rlog.SetLogLevel("error")
	features := tests.Features{
		ConsumerGroups:      true,
		ExactlyOnceDelivery: false,
		GuaranteedOrder:     false,
		Persistent:          true,
	}

	tests.TestPubSub(
		t,
		features,
		createPubSub,
		createPubSubWithConsumerGrup,
	)
}

func createPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return createPubSubWithConsumerGrup(t, "testGroup_"+string(tests.NewTestID()))
}

func createPubSubWithConsumerGrup(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
	return newPubSub(t, rocketmq.DefaultMarshaler{}, consumerGroup)
}

func newPubSub(t *testing.T, marshaler rocketmq.Marshaler, consumerGroup string) (message.Publisher, message.Subscriber) {
	PublisherConfig := rocketmq.DefaultPublisherConfig(roctetMQBrokers()...)
	PublisherConfig.Option = append(PublisherConfig.Option, producer.WithGroupName(consumerGroup))
	publisher, err := rocketmq.NewPublisher(
		PublisherConfig,
		watermill.NewStdLogger(false, false),
	)
	if err != nil {
		t.Fatal(err)
	}

	subscriber, err := rocketmq.NewSubscriber(
		rocketmq.DefaultSubscriberConfig(consumerGroup, roctetMQBrokers()...),
		watermill.NewStdLogger(false, false),
	)
	if err != nil {
		t.Fatal(err)
	}

	return publisher, subscriber
}

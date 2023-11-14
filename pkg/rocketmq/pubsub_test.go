package rocketmq_test

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/jeffmingup/watermill-rocketmq/pkg/rocketmq"
)

func roctetMQBrokers() []string {
	brokers := os.Getenv("WATERMILL_TEST_ROCTETMQ_BROKERS")
	if brokers != "" {
		return strings.Split(brokers, ",")
	}
	return []string{"192.168.169.203:9876"}
}

func TestPublishSubscribe(t *testing.T) {
	//rlog.SetLogLevel("error")
	features := tests.Features{
		ConsumerGroups:      true,
		ExactlyOnceDelivery: false,
		GuaranteedOrder:     true,
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
	name := strings.ReplaceAll(t.Name(), "-", "_")
	name = strings.ReplaceAll(name, "/", "_")
	return createPubSubWithConsumerGrup(t, "testGroup_ming_"+name)
}

func createPubSubWithConsumerGrup(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
	return newPubSub(t, rocketmq.DefaultMarshaler{}, consumerGroup)
}

func newPubSub(t *testing.T, marshaler rocketmq.Marshaler, consumerGroup string) (message.Publisher, message.Subscriber) {
	PublisherConfig := rocketmq.DefaultPublisherConfig(roctetMQBrokers()...)
	PublisherConfig.Option = append(PublisherConfig.Option, producer.WithGroupName("testGroup_"+string(tests.NewTestID())))
	publisher, err := rocketmq.NewPublisher(
		PublisherConfig,
		watermill.NewStdLogger(false, false),
	)
	if err != nil {
		t.Fatal(err)
	}

	SubscriberConfig := rocketmq.DefaultSubscriberConfig(consumerGroup, roctetMQBrokers()...)
	SubscriberConfig.ConsumeOrderly = true
	SubscriberConfig.Option = append(SubscriberConfig.Option, consumer.WithConsumeMessageBatchMaxSize(50))
	subscriber, err := rocketmq.NewSubscriber(
		SubscriberConfig,
		watermill.NewStdLogger(true, true),
	)
	if err != nil {
		t.Fatal(err)
	}

	return publisher, subscriber
}

func TestDefaultSubscriberInitialize(t *testing.T) {
	SubscriberConfig := rocketmq.DefaultSubscriberConfig("test", roctetMQBrokers()...)
	SubscriberConfig.ConsumeOrderly = true
	subscriber, err := rocketmq.NewSubscriber(
		SubscriberConfig,
		watermill.NewStdLogger(true, true),
	)
	if err != nil {
		t.Fatal(err)
	}

	topicName := "topic_" + string(tests.NewTestID())
	wg := sync.WaitGroup{}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func(i int) {
			if err := subscriber.SubscribeInitialize(fmt.Sprintf("%v_%v", topicName, i)); err != nil {
				t.Error(err)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

}

func TestPoll(t *testing.T) {
	SubscriberConfig := rocketmq.DefaultSubscriberConfig("test_ming_1", roctetMQBrokers()...)
	SubscriberConfig.ConsumeOrderly = true
	SubscriberConfig.Option = append(SubscriberConfig.Option, consumer.WithConsumeMessageBatchMaxSize(50))
	subscriber, err := rocketmq.NewSubscriber(
		SubscriberConfig,
		watermill.NewStdLogger(false, false),
	)
	if err != nil {
		t.Fatal(err)
	}

	PublisherConfig := rocketmq.DefaultPublisherConfig(roctetMQBrokers()...)
	PublisherConfig.Option = append(PublisherConfig.Option, producer.WithGroupName("testGroup_"+string(tests.NewTestID())))
	publisher, err := rocketmq.NewPublisher(
		PublisherConfig,
		watermill.NewStdLogger(false, false),
	)
	if err != nil {
		t.Fatal(err)
	}

	messagesCount := 50
	topicNum := 10
	var messagesToPublish []*message.Message
	for i := 0; i < messagesCount; i++ {
		id := watermill.NewUUID() + "__" + strconv.Itoa(i)

		msg := message.NewMessage(id, nil)
		messagesToPublish = append(messagesToPublish, msg)
	}
	topicName := "topic_" + watermill.NewUUID() + "__"
	for i := 0; i < topicNum; i++ {

		go func(i int) {
			tName := topicName + strconv.Itoa(i)
			if err := publisher.Publish(tName, messagesToPublish...); err != nil {
				t.Fatal(err)
			}

			msgChan, err := subscriber.Subscribe(context.Background(), tName)
			if err != nil {
				t.Fatal(err)
			}
			for msg := range msgChan {
				log.Println(msg.UUID, string(msg.Payload))
				msg.Ack()
			}
		}(i)
	}

	time.Sleep(1 * time.Hour)

}

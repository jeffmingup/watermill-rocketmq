package rocketmq_test

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2/admin"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/rlog"
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

func rocketMQAddr() []string {
	addr := os.Getenv("WATERMILL_TEST_ROCTETMQ_Addr")
	if addr != "" {
		return strings.Split(addr, ",")
	}
	return []string{"127.0.0.1:9876"}
}

func rocketMQBrokerAddr() string {
	addr := os.Getenv("WATERMILL_TEST_ROCTETMQ_BROCKER_Addr")
	if addr != "" {
		return addr
	}
	return "127.0.0.1:10911"
}

func TestPublishSubscribe(t *testing.T) {
	rlog.SetLogLevel("error")
	features := tests.Features{
		ConsumerGroups:                   true,
		ExactlyOnceDelivery:              false,
		GuaranteedOrder:                  true,
		Persistent:                       true,
		RequireSingleInstance:            true,
		NewSubscriberReceivesOldMessages: false,
		//RestartServiceCommand:            []string{"docker", "restart", "rmqbroker"},
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
	return newPubSub(t, consumerGroup)
}

func newPubSub(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
	PublisherConfig := rocketmq.DefaultPublisherConfig(rocketMQAddr()...)
	publisher, err := rocketmq.NewPublisher(
		PublisherConfig,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	SubscriberConfig := rocketmq.DefaultSubscriberConfig(consumerGroup, rocketMQAddr()...)
	SubscriberConfig.ConsumeOrderly = true
	SubscriberConfig.NackResendSleep = 0

	SubscriberConfig.Option = append(SubscriberConfig.Option, consumer.WithConsumeMessageBatchMaxSize(50))
	SubscriberConfig.InitializeTopicOptions = []admin.OptionCreate{admin.WithBrokerAddrCreate(rocketMQBrokerAddr())}
	subscriber, err := rocketmq.NewSubscriber(
		SubscriberConfig,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	return publisher, subscriber
}

func TestDefaultSubscriberInitialize(t *testing.T) {
	SubscriberConfig := rocketmq.DefaultSubscriberConfig("test", rocketMQAddr()...)
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
	SubscriberConfig := rocketmq.DefaultSubscriberConfig("test_ming_1", rocketMQAddr()...)
	SubscriberConfig.ConsumeOrderly = true
	SubscriberConfig.Option = append(SubscriberConfig.Option, consumer.WithConsumeMessageBatchMaxSize(50))
	subscriber, err := rocketmq.NewSubscriber(
		SubscriberConfig,
		watermill.NewStdLogger(false, false),
	)
	if err != nil {
		t.Fatal(err)
	}

	PublisherConfig := rocketmq.DefaultPublisherConfig(rocketMQAddr()...)
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
				t.Error(err)
			}

			msgChan, err := subscriber.Subscribe(context.Background(), tName)
			if err != nil {
				t.Error(err)
			}
			for msg := range msgChan {
				log.Println(msg.UUID, string(msg.Payload))
				msg.Ack()
			}
		}(i)
	}

	time.Sleep(1 * time.Hour)

}

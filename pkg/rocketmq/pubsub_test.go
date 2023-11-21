package rocketmq_test

import (
	"fmt"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
	"github.com/apache/rocketmq-client-go/v2/admin"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/rlog"
	"github.com/jeffmingup/watermill-rocketmq/pkg/rocketmq"
	"os"
	"strings"
	"sync"
	"testing"
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
		GuaranteedOrder:                  false,
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
	return createPubSubWithConsumerGrup(t, "test_group_"+name)
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

	SubscriberConfig.Option = append(SubscriberConfig.Option, consumer.WithConsumeMessageBatchMaxSize(50),
		consumer.WithMaxReconsumeTimes(200)) // 配合测试TestContinueAfterErrors的nacksPerSubscriber次数
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
	SubscriberConfig := rocketmq.DefaultSubscriberConfig("test_group", rocketMQAddr()...)
	subscriber, err := rocketmq.NewSubscriber(
		SubscriberConfig,
		nil,
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

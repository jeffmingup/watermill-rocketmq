package main

import (
	"context"
	"log"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/jeffmingup/watermill-rocketmq/pkg/rocketmq"
)

func main() {
	config := rocketmq.DefaultSubscriberConfig("test_group", "127.0.0.1:9876")
	config.Option = append(config.Option, consumer.WithConsumerOrder(true)) // 设置顺序消费
	subscriber, err := rocketmq.NewSubscriber(
		config,
		watermill.NewStdLogger(true, true),
	)
	if err != nil {
		panic(err)
	}
	msgs, err := subscriber.Subscribe(context.Background(), "topic_test")
	if err != nil {
		panic(err)
	}
	go func() {
		time.Sleep(10 * time.Second)
		err = subscriber.Close()
		if err != nil {
			panic(err)
		}
	}()
	for msg := range msgs {
		log.Println(string(msg.Payload))
		msg.Ack()
	}
}

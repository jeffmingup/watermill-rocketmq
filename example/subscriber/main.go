package main

import (
	"context"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/jeffmingup/watermill-rocketmq/pkg/rocketmq"
	"log"
	"time"
)

func main() {
	config := rocketmq.DefaultSubscriberConfig("testGroupming3", "127.0.0.1:9876")
	config.Option = append(config.Option, consumer.WithConsumerOrder(true)) // 设置顺序消费

	subscriber, err := rocketmq.NewSubscriber(
		config,
		watermill.NewStdLogger(true, true),
	)
	if err != nil {
		panic(err)
	}
	msgs, err := subscriber.Subscribe(context.Background(), "topic_003c4a45-2f9a-4385-ac2d-60b3d8325205")
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

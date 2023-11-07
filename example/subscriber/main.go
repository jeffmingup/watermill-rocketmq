package main

import (
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/jeffmingup/watermill-rocketmq/pkg/rocketmq"
)

func main() {
	subscriber, err := rocketmq.NewSubscriber(watermill.NewStdLogger(true, true))
	if err != nil {
		panic(err)
	}
	msgs, err := subscriber.Subscribe("ming")
	if err != nil {
		panic(err)
	}
	for msg := range msgs {
		println(string(msg.Payload))
		msg.Nack()
		time.Sleep(1 * time.Second)
	}
}

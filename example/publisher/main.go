package main

import (
	"fmt"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/jeffmingup/watermill-rocketmq/pkg/rocketmq"
)

func main() {
	publisher, err := rocketmq.NewPublisher(
		rocketmq.DefaultPublisherConfig("127.0.0.1:9876"),
		watermill.NewStdLogger(true, true),
	)
	if err != nil {
		panic(err)
	}
	for i := 0; i < 9999999; i++ {
		msg := message.NewMessage(watermill.NewUUID(),
			[]byte(fmt.Sprintf("Hello, world! %v", i)))
		msg.Metadata.Set("SHARDING_KEY", "1") // 顺序消息需要设置 SHARDING_KEY,否则随机分配队列
		if err := publisher.Publish("ming", msg); err != nil {
			panic(err)
		}
		time.Sleep(time.Second)
	}
}

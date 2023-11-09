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
		if err := publisher.Publish("topic_eae6d483-7b43-4b1b-a7d0-b62223480f3d_5", msg); err != nil {
			panic(err)
		}
		time.Sleep(time.Second)
	}
}

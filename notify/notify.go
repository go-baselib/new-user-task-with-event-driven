package notify

import (
	"context"
	"encoding/json"
	"log"

	"github.com/go-baselib/new-user-task-with-event-driven/consumer"
	"github.com/go-baselib/new-user-task-with-event-driven/producer"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

const Topic string = "notify"

const ConsumerGroup = "notify-consumer"

type Message struct {
	User string `json:"user"`
}

// Processor 消息处理器
func Processor(ctx context.Context) {
	var c, err = consumer.CreateSubscriber(ConsumerGroup, []string{"127.0.0.1:9092"})
	if err != nil {
		panic(err)
	}
	defer c.Close()

	var ch <-chan *message.Message
	ch, err = c.Subscribe(ctx, Topic)
	if err != nil {
		panic(err)
	}

	for msg := range ch {
		log.Printf("recv: %s \n", string(msg.Payload))
		var m Message
		if err = json.Unmarshal(msg.Payload, &m); err != nil {
			panic(err)
		}
		log.Printf("欢迎新用户：%s\n", m.User)
		msg.Ack()
	}
}

var publisher message.Publisher

func InitPublish(hosts []string) {
	var err error
	publisher, err = producer.CreatePublisher(hosts)
	if err != nil {
		panic(err)
	}
}

func (m Message) Publish(ctx context.Context) (string, error) {

	var (
		data []byte
		err  error
	)
	if data, err = json.Marshal(m); err != nil {
		panic(err)
	}

	var uuid = watermill.NewUUID()
	err = publisher.Publish(Topic, message.NewMessage(
		uuid, // internal uuid of the message, useful for debugging
		data,
	))
	if err != nil {
		return uuid, err
	}
	return uuid, nil
}

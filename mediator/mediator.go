package mediator

import (
	"context"
	"encoding/json"
	"log"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/go-baselib/new-user-task-with-event-driven/consumer"
	"github.com/go-baselib/new-user-task-with-event-driven/notify"
	"github.com/go-baselib/new-user-task-with-event-driven/producer"
	"github.com/go-baselib/postgresql-cdc/model"
)

const Topic = "sample"

const ConsumerGroup = "mediator-consumer"

func Listen(ctx context.Context) {
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
		log.Printf("payload: %s\n", string(msg.Payload))

		// 解析数据
		var (
			changes = model.Wal2JsonMessage{}
		)
		if err = json.Unmarshal(msg.Payload, &changes); err != nil {
			panic(err)
		}

		var nm = notify.Message{}
		for _, cg := range changes.Change {
			if cg.Schema != "sample" || cg.Table != "user" || cg.Kind != "insert" {
				continue
			}
			var ok bool
			for i, columnName := range cg.ColumnNames {
				if columnName != "name" {
					continue
				}
				if len(cg.ColumnValues) > i {
					nm.User, ok = cg.ColumnValues[i].(string)
					if !ok {
						break
					}
				}
			}

			if !ok {
				continue
			}
			var data []byte
			if data, err = json.Marshal(nm); err != nil {
				panic(err)
			}
			// 投递任务到NotifyChannel
			if err = NotifyEvent(ctx, data); err != nil {
				panic(err)
			}
		}

		msg.Ack()
	}
}

func NotifyEvent(ctx context.Context, data []byte) error {
	var publisher, err = producer.CreatePublisher([]string{"127.0.0.1:9092"})
	if err != nil {
		return err
	}
	err = publisher.Publish(notify.Topic, message.NewMessage(
		watermill.NewUUID(), // internal uuid of the message, useful for debugging
		data,
	))
	if err != nil {
		return err
	}
	return nil
}

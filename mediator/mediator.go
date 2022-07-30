package mediator

import (
	"context"
	"encoding/json"
	"log"

	"github.com/go-baselib/new-user-task-with-event-driven/consumer"
	"github.com/go-baselib/new-user-task-with-event-driven/notify"
	"github.com/go-baselib/new-user-task-with-event-driven/prom/user"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/go-baselib/postgresql-cdc/model"
)

const Topic = "sample"

const ConsumerGroup = "mediator-consumer"

func Listen(ctx context.Context) {

	var hosts = []string{"127.0.0.1:9092"}
	notify.InitPublish(hosts)
	user.InitPublish(hosts)

	var c, err = consumer.CreateSubscriber(ConsumerGroup, hosts)
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

		var (
			nm = notify.Message{}
			um = user.Message{}
		)
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
					var username string
					username, ok = cg.ColumnValues[i].(string)
					if !ok {
						break
					}
					nm.User = username
					um.CounterName = user.NewUser
				}
			}

			if !ok {
				continue
			}

			var uuid string
			if uuid, err = nm.Publish(ctx); err != nil {
				panic(err)
			}
			log.Printf("notify event uuid:%s \n", uuid)

			if uuid, err = um.Publish(ctx); err != nil {
				panic(err)
			}
			log.Printf("user event uuid:%s \n", uuid)
		}

		msg.Ack()
	}
}

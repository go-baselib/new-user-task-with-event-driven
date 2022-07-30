package user

import (
	"context"
	"encoding/json"
	"log"

	"github.com/go-baselib/new-user-task-with-event-driven/consumer"
	"github.com/go-baselib/new-user-task-with-event-driven/producer"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/prometheus/client_golang/prometheus"
	rkprom "github.com/rookie-ninja/rk-prom"
)

var metricsSet *rkprom.MetricsSet

type CounterName string

const (
	NewUser CounterName = "newuser"
)

var counterNames = [...]CounterName{NewUser}

func Init(registerer prometheus.Registerer) {
	metricsSet = rkprom.NewMetricsSet("dev", "user", registerer)

	for _, c := range counterNames {
		var err = metricsSet.RegisterCounter(string(c))
		if err != nil {
			panic(err)
		}
	}

	go Processor(context.Background())
}

func Report(c CounterName) {
	metricsSet.GetCounterWithValues(string(c)).Inc()
}

const Topic string = "user"

const ConsumerGroup = "user-consumer"

type Message struct {
	CounterName CounterName `json:"counter_name"`
}

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

		Report(m.CounterName)

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

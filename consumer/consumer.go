package consumer

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
)

// CreateSubscriber is a helper function similar to the previous one, but in this case it creates a Subscriber.
func CreateSubscriber(consumerGroup string, hosts []string) (message.Subscriber, error) {
	return kafka.NewSubscriber(
		kafka.SubscriberConfig{
			Brokers:       hosts,
			Unmarshaler:   kafka.DefaultMarshaler{},
			ConsumerGroup: consumerGroup, // every handler will use a separate consumer group
		},
		watermill.NopLogger{},
	)
}

package producer

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
)

// CreatePublisher is a helper function that creates a Publisher
func CreatePublisher(hosts []string) (message.Publisher, error) {
	return kafka.NewPublisher(
		kafka.PublisherConfig{
			Brokers:   hosts,
			Marshaler: kafka.DefaultMarshaler{},
		},
		watermill.NopLogger{},
	)
}

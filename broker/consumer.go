package broker

import "github.com/google/uuid"

type Consumer interface {
	Consume(Message) error
	Ack(Topic, uuid.UUID)
}

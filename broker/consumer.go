package broker

import (
	"fmt"
)

type Consumer interface {
	Consume(Payload) error
}

type PayloadConsumer struct{}

func (c PayloadConsumer) Consume(payload Payload) {
	fmt.Printf("consuming: %s\n", string(payload.Data))
}

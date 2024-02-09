package broker

import (
	"fmt"
)

type Consumer interface {
	Consume(Payload)
}

type PayloadConsumer struct{}

func (c PayloadConsumer) Consume(payload Payload) {
	fmt.Printf("consuming: %s\n", string(payload.Data))
}

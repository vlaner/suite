package broker

import (
	"fmt"
)

type Consumer struct {
}

func (c Consumer) Consume(payload Payload) {
	fmt.Printf("consuming: %s\n", string(payload.data))
}

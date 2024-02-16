package server

import (
	"log"
	"net"

	"github.com/vlaner/suite/broker"
)

// client kind
const (
	UNASSIGNED = -1
	PRODUCER   = 0
	CONSUMER   = 1
)

type Client struct {
	id   int
	conn net.Conn
	kind int
	e    *broker.Exchange
}

func NewClient(id int, conn net.Conn, e *broker.Exchange) *Client {
	client := Client{
		id:   id,
		conn: conn,
		kind: UNASSIGNED,
		e:    e,
	}

	return &client
}

func (c *Client) makeProducer() {
	if c.kind == UNASSIGNED {
		c.kind = PRODUCER
	}
}

func (c *Client) makeConsumer() {
	if c.kind == UNASSIGNED {
		c.kind = CONSUMER
	}
}

func (c Client) Consume(payload broker.Payload) {
	if c.kind == CONSUMER {
		_, err := c.conn.Write(append(payload.Data, []byte("\n")...))

		if err != nil {
			log.Printf("error writing payload bytes to %d: %s\n", c.id, err)
		}
	}
}

func (c Client) Publish(topic broker.Topic, data []byte) {
	if c.kind == PRODUCER {
		c.e.Publish(topic, broker.Payload{Data: data})
	}
}

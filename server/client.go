package server

import (
	"log"
	"net"

	"github.com/vlaner/suite/broker"
	"github.com/vlaner/suite/protocol"
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
	w    *protocol.Writer
}

func NewClient(id int, conn net.Conn, e *broker.Exchange) *Client {
	client := Client{
		id:   id,
		conn: conn,
		kind: UNASSIGNED,
		e:    e,
		w:    protocol.NewProtoWriter(conn),
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
		err := c.w.Write(protocol.Value{ValType: protocol.BINARY_STRING, Str: string(payload.Data)})
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

package server

import (
	"fmt"
	"log"
	"net"

	"github.com/google/uuid"
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

func (c Client) Consume(msg broker.Message) error {
	if c.kind == CONSUMER {
		err := c.w.Write(
			protocol.Value{ValType: protocol.ARRAY, Array: []protocol.Value{
				{ValType: protocol.BINARY_STRING, Str: "realm"},
				{ValType: protocol.BINARY_STRING, Str: "broker"},
				{ValType: protocol.BINARY_STRING, Str: "message_id"},
				{ValType: protocol.BINARY_STRING, Str: msg.Id.String()},
				{ValType: protocol.BINARY_STRING, Str: "data"},
				{ValType: protocol.BINARY_STRING, Str: string(msg.Data)},
			}})
		if err != nil {
			log.Printf("error writing payload bytes to %d: %s\n", c.id, err)
			return fmt.Errorf("error consuming message: %w", err)
		}
	}

	return nil
}

func (c Client) Publish(topic broker.Topic, data []byte) {
	if c.kind == PRODUCER {
		c.e.Publish(topic, data)
	}
}

func (c Client) Ack(topic broker.Topic, msgId uuid.UUID) {
	if c.kind == CONSUMER {
		c.e.Ack(topic, msgId)
	}
}

func (c Client) Unsubscribe(topic broker.Topic) {
	if c.kind == CONSUMER {
		c.e.Unsubscribe(topic)
	}
}

package server

import (
	"log"
	"net"

	"github.com/google/uuid"
	"github.com/vlaner/suite/broker"
	"github.com/vlaner/suite/protocol"
)

type Client struct {
	id   uuid.UUID
	conn net.Conn
	e    *broker.Exchange
	w    *protocol.Writer
	ch   chan broker.Message
}

func NewClient(id uuid.UUID, conn net.Conn, e *broker.Exchange) *Client {
	client := Client{
		id:   id,
		conn: conn,
		e:    e,
		w:    protocol.NewProtoWriter(conn),
		ch:   make(chan broker.Message),
	}

	go client.startConsuming()

	return &client
}

func (c Client) startConsuming() {
	for msg := range c.ch {
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
		}
	}
}

func (c Client) Chan() chan broker.Message {
	return c.ch
}

func (c Client) ID() uuid.UUID {
	return c.id
}

func (c Client) Publish(topic broker.Topic, data []byte) {
	c.e.Publish(topic, data)
}

func (c Client) Unsubscribe(topic broker.Topic) {
	c.e.Unsubscribe(topic, c)
}

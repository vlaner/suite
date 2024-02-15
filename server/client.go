package server

import (
	"fmt"
	"net"

	"github.com/vlaner/suite/broker"
)

const (
	PRODUCER = 0
	CONSUMER = 1
)

type Client struct {
	id   int
	conn net.Conn
	// 0 - producer, 1 - consumer
	kind int
	e    *broker.Exchange
}

func NewClient(id int, conn net.Conn, kind int, e *broker.Exchange) *Client {
	client := Client{
		id:   id,
		conn: conn,
		kind: kind,
		e:    e,
	}

	return &client
}

func (c *Client) makeProducer() {
	c.kind = PRODUCER
}

func (c *Client) makeConsumer() {
	c.kind = CONSUMER
}

func (c Client) Consume(payload broker.Payload) {
	if c.kind == CONSUMER {
		fmt.Println("DATA FROM TCP CONSUMER: ", string(payload.Data))
	}
}

func (c Client) Publish(topic broker.Topic, data []byte) {
	if c.kind == PRODUCER {
		c.e.Publish(topic, broker.Payload{Data: data})
	}
}

package server

import (
	"net"

	"github.com/vlaner/suite/broker"
)

type Client struct {
	id   int
	conn net.Conn
	e    *broker.Exchange
}

func NewClient(id int, conn net.Conn, e *broker.Exchange) *Client {
	client := Client{
		id:   id,
		conn: conn,
		e:    e,
	}

	return &client
}

func (c Client) Consume(payload broker.Payload) {
	payload.Data = append(payload.Data[:], []byte("\n")...)
	c.conn.Write(payload.Data)
}

func (c Client) Publish(topic broker.Topic, data []byte) {
	c.e.Publish(topic, broker.Payload{Data: data})
}

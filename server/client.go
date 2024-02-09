package server

import (
	"net"

	"github.com/vlaner/suite/broker"
)

type Client struct {
	id   int
	conn net.Conn
}

func NewClient(id int, conn net.Conn) *Client {
	client := Client{
		id:   id,
		conn: conn,
	}

	return &client
}

func (c Client) Consume(payload broker.Payload) {
	payload.Data = append(payload.Data[:], []byte("\n")...)
	c.conn.Write(payload.Data)
}

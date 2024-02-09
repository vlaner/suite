package server

import (
	"bufio"
	"io"
	"net"
	"testing"

	"github.com/vlaner/suite/broker"
)

const SERVER_ADDR = ":8000"

type testClient struct {
	id   int
	conn net.Conn
}

func (tc *testClient) Stop() {
	tc.conn.Close()
}

func newTestClient(srvAddr string) (*testClient, error) {
	conn, err := net.Dial("tcp", ":8000")
	if err != nil {
		return nil, err
	}

	return &testClient{
		conn: conn,
	}, nil
}

func TestServerHandlesMessage(t *testing.T) {
	// sync := make(chan struct{})
	srv := NewTcpServer(SERVER_ADDR, nil)

	srv.Start()
	defer srv.Stop()

	c, err := newTestClient(SERVER_ADDR)
	if err != nil {
		t.Errorf("error connecting to server: %s", err)
	}
	defer c.Stop()

	// buf := make([]byte, 256)
	// go func() {
	// 	_, err := c.conn.Read(buf)
	// 	if err != nil {
	// 		if err != io.EOF {
	// 			t.Errorf("read error: %s", err)
	// 		}
	// 		return
	// 	}
	// 	t.Logf("got response: %s", string(buf[:]))
	// 	sync <- struct{}{}
	// }()

	// <-sync
}

func TestServerMessageExchange(t *testing.T) {
	var tcpConsumer *Client
	msgsCount := 5
	msgsProcessed := 0
	e := broker.NewExchange()
	topic := broker.Topic("tcpserver")

	srv := NewTcpServer(SERVER_ADDR, e)
	srv.Start()
	defer srv.Stop()

	c, err := newTestClient(SERVER_ADDR)
	if err != nil {
		t.Errorf("error connecting to server: %s", err)
	}
	defer c.Stop()
	found := false
	for !found {
		for _, tcpClients := range srv.clients {
			tcpConsumer = tcpClients
		}
		if tcpConsumer != nil {
			found = true
		}
	}
	reader := bufio.NewReader(c.conn)
	sync := make(chan struct{})
	go func() {
		for {
			buf, _, err := reader.ReadLine()
			if err != nil {
				if err != io.EOF {
					t.Errorf("read error: %s", err)
				}
				return
			}
			t.Logf("got response: %s\n", string(buf[:]))
			msgsProcessed++
			if msgsProcessed == msgsCount {
				sync <- struct{}{}
				break
			}
		}
	}()

	e.ListenForMessages()
	e.Subscribe(topic, tcpConsumer)
	for i := 0; i < 5; i++ {
		e.Publish(topic, broker.Payload{Data: []byte("testing tcp send")})
	}

	<-sync
}

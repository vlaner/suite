package server

import (
	"bufio"
	"net"
	"testing"
	"time"

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
func (tc *testClient) waitForMessages(t *testing.T, amount int) {
	r := bufio.NewReader(tc.conn)

	for i := 0; i < amount; i++ {
		b, _, err := r.ReadLine()
		if err != nil {
			t.Errorf("error reading from server: %s", err)
		}

		t.Logf("got message from server: %s", string(b))
	}
}

func newTestClient(srvAddr string) (*testClient, error) {
	conn, err := net.Dial("tcp", srvAddr)
	if err != nil {
		return nil, err
	}

	time.Sleep(10 * time.Millisecond)

	return &testClient{
		conn: conn,
	}, nil
}

func TestServerHandlesMessage(t *testing.T) {
	srv := NewTcpServer(SERVER_ADDR, nil)

	srv.Start()
	defer srv.Stop()

	c, err := newTestClient(SERVER_ADDR)
	if err != nil {
		t.Errorf("error connecting to server: %s", err)
	}
	defer c.Stop()
}

func TestServerMessageExchange(t *testing.T) {
	e := broker.NewExchange()
	e.ListenForMessages()

	topic := broker.Topic("TESTTOPIC")

	srv := NewTcpServer(SERVER_ADDR, e)
	srv.Start()
	defer srv.Stop()

	c, err := newTestClient(SERVER_ADDR)
	if err != nil {
		t.Errorf("error connecting to server: %s", err)
	}
	defer c.Stop()

	c.conn.Write([]byte("consume TESTTOPIC\n"))

	time.Sleep(100 * time.Millisecond)

	msgsCount := 5
	for i := 0; i < msgsCount; i++ {
		e.Publish(topic, broker.Payload{Data: []byte("testing tcp send directly from exchange")})
	}

	c.waitForMessages(t, msgsCount)
}

func TestServerConsumerAndProducer(t *testing.T) {
	e := broker.NewExchange()
	e.ListenForMessages()

	srv := NewTcpServer(SERVER_ADDR, e)
	srv.Start()
	defer srv.Stop()

	consumer, err := newTestClient(SERVER_ADDR)
	if err != nil {
		t.Errorf("error connecting to server: %s", err)
	}
	defer consumer.Stop()

	producer, err := newTestClient(SERVER_ADDR)
	if err != nil {
		t.Errorf("error connecting to server: %s", err)
	}
	defer producer.Stop()

	consumer.conn.Write([]byte("consume TESTTOPIC\n"))
	producer.conn.Write([]byte("producer TESTTOPIC\n"))

	time.Sleep(100 * time.Millisecond)

	msgsCount := 5
	for i := 0; i < msgsCount; i++ {
		producer.conn.Write([]byte("publish TESTTOPIC data from producer over tcp\n"))
	}

	consumer.waitForMessages(t, msgsCount)
}

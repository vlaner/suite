package server

import (
	"bufio"
	"net"
	"os"
	"testing"
	"time"

	"github.com/vlaner/suite/broker"
	"github.com/vlaner/suite/database"
)

const SERVER_ADDR = ":8000"

type testClient struct {
	id    int
	conn  net.Conn
	msgCh chan string
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

		tc.msgCh <- string(b)
	}

	close(tc.msgCh)
}

func newTestClient(srvAddr string) (*testClient, error) {
	conn, err := net.Dial("tcp", srvAddr)
	if err != nil {
		return nil, err
	}

	time.Sleep(10 * time.Millisecond)

	return &testClient{
		conn:  conn,
		msgCh: make(chan string),
	}, nil
}

func TestServerHandlesMessage(t *testing.T) {
	srv := NewTcpServer(SERVER_ADDR, nil, nil)

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

	srv := NewTcpServer(SERVER_ADDR, e, nil)
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

	go c.waitForMessages(t, msgsCount)
	for msg := range c.msgCh {
		t.Logf("got message from server: %s", msg)
	}
}

func TestServerConsumerAndProducer(t *testing.T) {
	e := broker.NewExchange()
	e.ListenForMessages()

	srv := NewTcpServer(SERVER_ADDR, e, nil)
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

	go consumer.waitForMessages(t, msgsCount)
	for msg := range consumer.msgCh {
		t.Logf("got message from server: %s", msg)
	}
}

func TestServerWithDatabaseGetSet(t *testing.T) {
	dirPath := "testdb"
	defer os.RemoveAll(dirPath)

	db, err := database.Open(dirPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	srv := NewTcpServer(SERVER_ADDR, nil, db)
	srv.Start()
	defer srv.Stop()

	c, err := newTestClient(SERVER_ADDR)
	if err != nil {
		t.Errorf("error connecting to server: %s", err)
	}
	defer c.Stop()

	c.conn.Write([]byte("set testkey testval test 2\n"))
	c.conn.Write([]byte("get testkey\n"))
	go c.waitForMessages(t, 1)
	getResult := <-c.msgCh
	if getResult != "key: testkey; value: testval" {
		t.Errorf("unexpected 'get' result from server, wanted 'key: testkey; value: testval' but got %s", getResult)
	}
}

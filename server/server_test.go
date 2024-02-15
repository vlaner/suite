package server

import (
	"bufio"
	"io"
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

func newTestClient(srvAddr string) (*testClient, error) {
	conn, err := net.Dial("tcp", ":8000")
	if err != nil {
		return nil, err
	}
	time.Sleep(200 * time.Millisecond)
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
	msgsCount := 5
	msgsProcessed := 0
	e := broker.NewExchange()
	e.ListenForMessages()

	topic := broker.Topic("tcpserver")

	srv := NewTcpServer(SERVER_ADDR, e)
	srv.Start()
	defer srv.Stop()

	c, err := newTestClient(SERVER_ADDR)
	if err != nil {
		t.Errorf("error connecting to server: %s", err)
	}
	defer c.Stop()

	tcpConsumer := srv.getClientById(1)
	e.Subscribe(topic, tcpConsumer)

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

	for i := 0; i < msgsCount; i++ {
		e.Publish(topic, broker.Payload{Data: []byte("testing tcp send")})
	}

	<-sync
}

func TestServerConsumerAndProducer(t *testing.T) {
	msgsCount := 5
	e := broker.NewExchange()
	topic := broker.Topic("tcpserver")
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

	tcpConsumer := srv.getClientById(1)
	e.Subscribe(topic, tcpConsumer)

	tcpProducer := srv.getClientById(2)

	reader := bufio.NewReader(consumer.conn)
	syncConsumer := make(chan struct{})
	msgsProcessed := 0
	go func() {
		for {
			buf, _, err := reader.ReadLine()
			if err != nil {
				if err != io.EOF {
					t.Errorf("read error: %s", err)
				}
				return
			}
			t.Logf("consuming: %s\n", string(buf[:]))
			msgsProcessed++
			if msgsProcessed == msgsCount {
				syncConsumer <- struct{}{}
				break
			}
		}
	}()

	msgsSent := 0
	producerReader := bufio.NewReader(producer.conn)
	syncProducer := make(chan struct{})
	go func() {
		for {
			buf, _, err := producerReader.ReadLine()
			if err != nil {
				if err != io.EOF {
					t.Errorf("read error: %s", err)
				}
				return
			}
			tcpProducer.Publish(topic, buf)
			msgsSent++
			if msgsSent == msgsCount {
				syncProducer <- struct{}{}
				break
			}
		}
	}()

	for i := 0; i < msgsCount; i++ {
		tcpProducer.conn.Write([]byte("data from producer over tcp\n"))
	}

	<-syncConsumer
	<-syncProducer
}

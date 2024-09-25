package server

import (
	"bytes"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vlaner/suite/broker"
	"github.com/vlaner/suite/database"
	"github.com/vlaner/suite/protocol"
)

const SERVER_ADDR = ":8000"

func intToBytes(i int) []byte {
	return []byte(strconv.Itoa(i))
}

type testClient struct {
	id       uuid.UUID
	conn     net.Conn
	msgCh    chan protocol.Value
	brokerCh chan broker.Message
}

func (tc *testClient) Stop() {
	tc.conn.Close()
}

func (tc *testClient) waitForMessages(t *testing.T, amount int) {
	r := protocol.NewProtoReader(tc.conn)

	for i := 0; i < amount; i++ {
		protoVal, err := r.ParseInput()
		if err != nil {
			t.Errorf("error parsing input: %s", err)
		}

		tc.msgCh <- *protoVal
	}

	close(tc.msgCh)
}

func newTestClient(id uuid.UUID, srvAddr string) (*testClient, error) {
	conn, err := net.Dial("tcp", srvAddr)
	if err != nil {
		return nil, err
	}

	tc := testClient{
		id:    id,
		conn:  conn,
		msgCh: make(chan protocol.Value),
	}

	return &tc, nil
}

func TestServerHandlesMessage(t *testing.T) {
	srv := NewTcpServer(SERVER_ADDR, nil, nil)

	srv.Start()
	defer srv.Stop()

	c, err := newTestClient(uuid.New(), SERVER_ADDR)
	if err != nil {
		t.Errorf("error connecting to server: %s", err)
	}
	defer c.Stop()
	w := protocol.NewProtoWriter(c.conn)

	err = w.Write(protocol.Value{
		ValType: protocol.ARRAY,
		Str:     "",
		Array: []protocol.Value{
			{ValType: protocol.BINARY_STRING, Str: "teststring"},
		},
	})
	if err != nil {
		t.Errorf("error writing protocol data: %s", err)
	}
}

func TestServerMessageExchange(t *testing.T) {
	e := broker.NewExchange()

	topic := broker.Topic("TESTTOPIC")

	srv := NewTcpServer(SERVER_ADDR, e, nil)
	srv.Start()
	defer srv.Stop()

	msgsCount := 5
	c, err := newTestClient(uuid.New(), SERVER_ADDR)
	if err != nil {
		t.Errorf("error connecting to server: %s", err)
	}
	defer c.Stop()

	go c.waitForMessages(t, msgsCount)

	w := protocol.NewProtoWriter(c.conn)
	err = w.Write(protocol.Value{
		ValType: protocol.ARRAY,
		Str:     "",
		Array: []protocol.Value{
			{ValType: protocol.BINARY_STRING, Str: "consume"},
			{ValType: protocol.BINARY_STRING, Str: "TESTTOPIC"},
		},
	})
	if err != nil {
		t.Errorf("error writing protocol data: %s", err)
	}

	time.Sleep(200 * time.Millisecond)

	for i := 0; i < msgsCount; i++ {
		e.Publish(topic, []byte("testing tcp send directly from exchange"))
	}

	for msg := range c.msgCh {
		t.Logf("TestServerMessageExchange got message from server: %+v", msg)
	}
}

func TestServerConsumerAndProducer(t *testing.T) {
	e := broker.NewExchange()

	srv := NewTcpServer(SERVER_ADDR, e, nil)
	srv.Start()
	defer srv.Stop()

	consumer, err := newTestClient(uuid.New(), SERVER_ADDR)
	if err != nil {
		t.Errorf("error connecting to server: %s", err)
	}
	defer consumer.Stop()
	wConsumer := protocol.NewProtoWriter(consumer.conn)

	producer, err := newTestClient(uuid.New(), SERVER_ADDR)
	if err != nil {
		t.Errorf("error connecting to server: %s", err)
	}
	defer producer.Stop()
	wProducer := protocol.NewProtoWriter(producer.conn)

	err = wConsumer.Write(protocol.Value{
		ValType: protocol.ARRAY,
		Str:     "",
		Array: []protocol.Value{
			{ValType: protocol.BINARY_STRING, Str: "consume"},
			{ValType: protocol.BINARY_STRING, Str: "TESTTOPIC"},
		},
	})
	if err != nil {
		t.Errorf("error writing protocol data: %s", err)
	}

	err = wProducer.Write(protocol.Value{
		ValType: protocol.ARRAY,
		Str:     "",
		Array: []protocol.Value{
			{ValType: 1, Str: "producer"},
		},
	})
	if err != nil {
		t.Errorf("error writing protocol data: %s", err)
	}

	msgsCount := 5
	for i := 0; i < msgsCount; i++ {
		err = wProducer.Write(protocol.Value{
			ValType: protocol.ARRAY,
			Str:     "",
			Array: []protocol.Value{
				{ValType: protocol.BINARY_STRING, Str: "publish"},
				{ValType: protocol.BINARY_STRING, Str: "TESTTOPIC"},
				{ValType: protocol.BINARY_STRING, Str: "data from producer over tcp"},
			},
		})
		if err != nil {
			t.Errorf("error writing protocol data: %s", err)
		}
	}

	go consumer.waitForMessages(t, msgsCount)
	for msg := range consumer.msgCh {
		t.Logf("TestServerConsumerAndProducer got message from server: %+v", msg)
	}
}

func TestServerClientReceivesPayloadInOrder(t *testing.T) {
	e := broker.NewExchange()

	topic := broker.Topic("TESTTOPIC")

	srv := NewTcpServer(SERVER_ADDR, e, nil)

	srv.Start()
	defer srv.Stop()

	c, err := newTestClient(uuid.New(), SERVER_ADDR)
	if err != nil {
		t.Errorf("error connecting to server: %s", err)
	}
	defer c.Stop()

	msgsCount := 50
	go c.waitForMessages(t, msgsCount)

	w := protocol.NewProtoWriter(c.conn)

	err = w.Write(protocol.Value{
		ValType: protocol.ARRAY,
		Str:     "",
		Array: []protocol.Value{
			{ValType: protocol.BINARY_STRING, Str: "consume"},
			{ValType: protocol.BINARY_STRING, Str: "TESTTOPIC"},
		},
	})
	if err != nil {
		t.Errorf("error writing protocol data: %s", err)
	}

	time.Sleep(200 * time.Millisecond)

	for i := 0; i < msgsCount; i++ {
		e.Publish(topic, append([]byte("message #"), intToBytes(i)...))
	}

	for i := 0; i < msgsCount; i++ {
		gotData := <-c.msgCh
		want := append([]byte("message #"), intToBytes(i)...)
		if !bytes.Equal([]byte(gotData.Array[5].Str), want) {
			t.Errorf("consumer got unexpected data: want '%s' got '%s'", string(want), gotData.Array[5].Str)
		}
	}
}

// func TestServerClientAckMessage(t *testing.T) {
// 	e := broker.NewExchange()

// 	topic := broker.Topic("TESTTOPIC")

// 	srv := NewTcpServer(SERVER_ADDR, e, nil)

// 	srv.Start()
// 	defer srv.Stop()

// 	c, err := newTestClient(uuid.New(), SERVER_ADDR)
// 	if err != nil {
// 		t.Errorf("error connecting to server: %s", err)
// 	}
// 	defer c.Stop()

// 	w := protocol.NewProtoWriter(c.conn)
// 	err = w.Write(protocol.Value{
// 		ValType: protocol.ARRAY,
// 		Str:     "",
// 		Array: []protocol.Value{
// 			{ValType: protocol.BINARY_STRING, Str: "consume"},
// 			{ValType: protocol.BINARY_STRING, Str: "TESTTOPIC"},
// 		},
// 	})
// 	if err != nil {
// 		t.Errorf("error writing protocol data: %s", err)
// 	}

// 	msgsCount := 50
// 	for i := 0; i < msgsCount; i++ {
// 		e.Publish(topic, append([]byte("message #"), intToBytes(i)...))
// 	}

// 	go c.waitForMessages(t, msgsCount)
// 	for gotData := range c.msgCh {
// 		msgId := gotData.Array[3]

// 		err = w.Write(protocol.Value{
// 			ValType: protocol.ARRAY,
// 			Str:     "",
// 			Array: []protocol.Value{
// 				{ValType: protocol.BINARY_STRING, Str: "ack"},
// 				{ValType: protocol.BINARY_STRING, Str: "TESTTOPIC"},
// 				{ValType: protocol.BINARY_STRING, Str: msgId.Str},
// 			},
// 		})
// 		if err != nil {
// 			t.Errorf("error writing protocol data: %s", err)
// 		}
// 	}
// 	time.Sleep(10 * time.Millisecond)
// 	unacked := len(e.GetUnackedMessages(topic))
// 	if unacked != 0 {
// 		t.Error("got more than 0 unacked messages:", unacked)
// 	}
// }

func TestServerClientUnsubscribe(t *testing.T) {
	e := broker.NewExchange()

	topic := broker.Topic("TESTTOPIC")

	srv := NewTcpServer(SERVER_ADDR, e, nil)

	srv.Start()
	defer srv.Stop()
	c, err := newTestClient(uuid.New(), SERVER_ADDR)
	if err != nil {
		t.Errorf("error connecting to server: %s", err)
	}
	defer c.Stop()

	go c.waitForMessages(t, 1)

	w := protocol.NewProtoWriter(c.conn)
	err = w.Write(protocol.Value{
		ValType: protocol.ARRAY,
		Str:     "",
		Array: []protocol.Value{
			{ValType: protocol.BINARY_STRING, Str: "consume"},
			{ValType: protocol.BINARY_STRING, Str: "TESTTOPIC"},
		},
	})
	if err != nil {
		t.Errorf("error writing protocol data: %s", err)
	}
	time.Sleep(200 * time.Millisecond)

	e.Publish(topic, []byte("message #1"))

	<-c.msgCh

	err = w.Write(protocol.Value{
		ValType: protocol.ARRAY,
		Str:     "",
		Array: []protocol.Value{
			{ValType: protocol.BINARY_STRING, Str: "unsub"},
			{ValType: protocol.BINARY_STRING, Str: "TESTTOPIC"},
		},
	})
	if err != nil {
		t.Errorf("error writing protocol data: %s", err)
	}

	client2, err := newTestClient(uuid.New(), SERVER_ADDR)
	if err != nil {
		t.Errorf("error connecting to server: %s", err)
	}
	defer client2.Stop()
	go client2.waitForMessages(t, 3)

	w2 := protocol.NewProtoWriter(client2.conn)
	err = w2.Write(protocol.Value{
		ValType: protocol.ARRAY,
		Str:     "",
		Array: []protocol.Value{
			{ValType: protocol.BINARY_STRING, Str: "consume"},
			{ValType: protocol.BINARY_STRING, Str: "TESTTOPIC"},
		},
	})
	if err != nil {
		t.Errorf("error writing protocol data: %s", err)
	}

	time.Sleep(100 * time.Millisecond)

	e.Publish(topic, []byte("message #1"))
	e.Publish(topic, []byte("message #2"))
	e.Publish(topic, []byte("message #3"))

	for i := 1; i <= 3; i++ {
		gotData := <-client2.msgCh
		want := append([]byte("message #"), intToBytes(i)...)
		t.Log("here", string(gotData.Str), string(want))
		if !bytes.Equal([]byte(gotData.Array[5].Str), want) {
			t.Errorf("consumer got unexpected data: want '%s' got '%s'", string(want), gotData.Str)
		}
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

	c, err := newTestClient(uuid.New(), SERVER_ADDR)
	if err != nil {
		t.Errorf("error connecting to server: %s", err)
	}
	defer c.Stop()
	w := protocol.NewProtoWriter(c.conn)

	err = w.Write(protocol.Value{
		ValType: protocol.ARRAY,
		Array: []protocol.Value{
			{ValType: protocol.BINARY_STRING, Str: "set"},
			{ValType: protocol.BINARY_STRING, Str: "testkey"},
			{ValType: protocol.BINARY_STRING, Str: "testval test 2"},
		},
	})
	if err != nil {
		t.Errorf("error writing protocol data: %s", err)
	}

	err = w.Write(protocol.Value{
		ValType: protocol.ARRAY,
		Array: []protocol.Value{
			{ValType: protocol.BINARY_STRING, Str: "get"},
			{ValType: protocol.BINARY_STRING, Str: "testkey"},
		},
	})
	if err != nil {
		t.Errorf("error writing protocol data: %s", err)
	}

	go c.waitForMessages(t, 1)
	getResult := <-c.msgCh
	gotKey := []byte(getResult.Array[2].Str)
	gotValue := []byte(getResult.Array[3].Str)
	if !bytes.Equal(gotKey, []byte("testkey")) || !bytes.Equal(gotValue, []byte("testval test 2")) {
		t.Errorf("unexpected 'get' result from server, wanted key be: 'testkey' but got %s, want value be: 'testval test 2' but got %s", gotKey, gotValue)
	}
}

func TestServerWithDatabaseGetError(t *testing.T) {
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

	c, err := newTestClient(uuid.New(), SERVER_ADDR)
	if err != nil {
		t.Errorf("error connecting to server: %s", err)
	}
	defer c.Stop()

	w := protocol.NewProtoWriter(c.conn)

	err = w.Write(protocol.Value{
		ValType: protocol.ARRAY,
		Array: []protocol.Value{
			{ValType: protocol.BINARY_STRING, Str: "get"},
			{ValType: protocol.BINARY_STRING, Str: "testkey"},
		},
	})
	if err != nil {
		t.Errorf("error writing protocol data: %s", err)
	}

	go c.waitForMessages(t, 1)
	getResult := <-c.msgCh
	if getResult.Str != "key not found" {
		t.Errorf("unexpected 'get' result from server, wanted 'key not found' but got %s", getResult.Str)
	}
}

func TestServerWithDatabaseDelError(t *testing.T) {
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

	c, err := newTestClient(uuid.New(), SERVER_ADDR)
	if err != nil {
		t.Errorf("error connecting to server: %s", err)
	}
	defer c.Stop()
	w := protocol.NewProtoWriter(c.conn)

	err = w.Write(protocol.Value{
		ValType: protocol.ARRAY,
		Array: []protocol.Value{
			{ValType: protocol.BINARY_STRING, Str: "del"},
			{ValType: protocol.BINARY_STRING, Str: "testkey"},
		},
	})
	if err != nil {
		t.Errorf("error writing protocol data: %s", err)
	}

	go c.waitForMessages(t, 1)
	getResult := <-c.msgCh
	if getResult.Str != "cannot delete key" {
		t.Errorf("unexpected 'get' result from server, wanted 'cannot delete key' but got %s", getResult.Str)
	}
}

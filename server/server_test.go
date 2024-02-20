package server

import (
	"bytes"
	"net"
	"os"
	"strconv"
	"testing"

	"github.com/vlaner/suite/broker"
	"github.com/vlaner/suite/database"
	"github.com/vlaner/suite/protocol"
)

const SERVER_ADDR = ":8000"

func intToBytes(i int) []byte {
	return []byte(strconv.Itoa(i))
}

type testClient struct {
	id    int
	conn  net.Conn
	msgCh chan protocol.Value
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

func newTestClient(srvAddr string) (*testClient, error) {
	conn, err := net.Dial("tcp", srvAddr)
	if err != nil {
		return nil, err
	}

	return &testClient{
		conn:  conn,
		msgCh: make(chan protocol.Value),
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

	c, err := newTestClient(SERVER_ADDR)
	if err != nil {
		t.Errorf("error connecting to server: %s", err)
	}
	defer c.Stop()
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

	msgsCount := 5
	for i := 0; i < msgsCount; i++ {
		e.Publish(topic, []byte("testing tcp send directly from exchange"))
	}

	go c.waitForMessages(t, msgsCount)
	for msg := range c.msgCh {
		t.Logf("got message from server: %+v", msg)
	}
}

func TestServerConsumerAndProducer(t *testing.T) {
	e := broker.NewExchange()

	srv := NewTcpServer(SERVER_ADDR, e, nil)
	srv.Start()
	defer srv.Stop()

	consumer, err := newTestClient(SERVER_ADDR)
	if err != nil {
		t.Errorf("error connecting to server: %s", err)
	}
	defer consumer.Stop()
	wConsumer := protocol.NewProtoWriter(consumer.conn)

	producer, err := newTestClient(SERVER_ADDR)
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
		t.Logf("got message from server: %+v", msg)
	}
}

func TestServerClientReceivesPayloadInOrder(t *testing.T) {
	e := broker.NewExchange()

	topic := broker.Topic("TESTTOPIC")

	srv := NewTcpServer(SERVER_ADDR, e, nil)

	srv.Start()
	defer srv.Stop()

	c, err := newTestClient(SERVER_ADDR)
	if err != nil {
		t.Errorf("error connecting to server: %s", err)
	}
	defer c.Stop()
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

	msgsCount := 50
	for i := 0; i < msgsCount; i++ {
		e.Publish(topic, append([]byte("message #"), intToBytes(i)...))
	}

	go c.waitForMessages(t, msgsCount)
	for i := 0; i < msgsCount; i++ {
		gotData := <-c.msgCh
		want := append([]byte("message #"), intToBytes(i)...)
		if !bytes.Equal([]byte(gotData.Array[5].Str), want) {
			t.Errorf("consumer got unexpected data: want '%s' got '%s'", string(want), gotData.Array[5].Str)
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

	c, err := newTestClient(SERVER_ADDR)
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

	c, err := newTestClient(SERVER_ADDR)
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

	c, err := newTestClient(SERVER_ADDR)
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

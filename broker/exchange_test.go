package broker

import (
	"bytes"
	"strconv"
	"testing"
)

func intToBytes(i int) []byte {
	return []byte(strconv.Itoa(i))
}

type testConsumer struct {
	gotMsgs chan []byte
}

func newTestConsumer() *testConsumer {
	return &testConsumer{
		gotMsgs: make(chan []byte),
	}
}

func (c testConsumer) Consume(msg Message) error {
	c.gotMsgs <- msg.Data
	return nil
}

func TestBasicConsume(t *testing.T) {
	c := newTestConsumer()
	topic := Topic("test")
	e := NewExchange()

	e.Subscribe(topic, c)

	e.Publish(topic, []byte("testdata"))

	gotData := <-c.gotMsgs
	if !bytes.Equal(gotData, []byte("testdata")) {
		t.Errorf("consumer got unexpected data: want 'testdata' got %s", string(gotData))
	}
}

func TestSeveralMessages(t *testing.T) {
	c := newTestConsumer()

	topic := Topic("test")
	e := NewExchange()

	e.Subscribe(topic, c)

	for i := 0; i < 30; i++ {
		payloadBytes := append([]byte("testdata"), intToBytes(i)...)
		e.Publish(topic, payloadBytes)
	}

	for i := 0; i < 30; i++ {
		gotData := <-c.gotMsgs
		want := append([]byte("testdata"), intToBytes(i)...)
		if !bytes.Equal(gotData, want) {
			t.Errorf("consumer got unexpected data: want '%s' got '%s'", string(want), string(gotData))
		}
	}
}

func TestExchangeWithProducer(t *testing.T) {
	topic := Topic("test")
	e := NewExchange()

	c := newTestConsumer()
	p := PayloadProducer{e: e}

	e.Subscribe(topic, c)

	for i := 0; i < 30; i++ {
		payloadBytes := append([]byte("testdata"), intToBytes(i)...)
		p.Publish(topic, payloadBytes)
	}

	for i := 0; i < 30; i++ {
		gotData := <-c.gotMsgs
		want := append([]byte("testdata"), intToBytes(i)...)
		if !bytes.Equal(gotData, want) {
			t.Errorf("consumer got unexpected data: want '%s' got '%s'", string(want), string(gotData))
		}
	}
}

func TestExchangeStopWorks(t *testing.T) {
	c := newTestConsumer()

	topic := Topic("test")
	e := NewExchange()

	e.Subscribe(topic, c)

	e.Publish(topic, []byte("testdata"))
	e.Stop()
	e.Publish(topic, []byte("testdata"))
}

func TestExchangeSubscibeSecondCosnumerShouldNotWork(t *testing.T) {
	c := newTestConsumer()
	c2 := newTestConsumer()
	go func() {
		<-c2.gotMsgs
		t.Errorf("receiving message should not happen")
	}()

	topic := Topic("test")
	e := NewExchange()

	e.Subscribe(topic, c)
	e.Subscribe(topic, c2)

	e.Publish(topic, []byte("testdata"))
	<-c.gotMsgs
}

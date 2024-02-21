package broker

import (
	"bytes"
	"strconv"
	"sync"
	"testing"

	"github.com/google/uuid"
)

func intToBytes(i int) []byte {
	return []byte(strconv.Itoa(i))
}

type testConsumer struct {
	gotMsgs chan Message
	e       *Exchange
}

func newTestConsumer(e *Exchange) *testConsumer {
	return &testConsumer{
		gotMsgs: make(chan Message),
		e:       e,
	}
}

func (c testConsumer) Consume(msg Message) error {
	c.gotMsgs <- msg
	return nil
}

func (c testConsumer) Ack(topic Topic, msgId uuid.UUID) {
	c.e.Ack(topic, msgId)
}

func TestBasicConsume(t *testing.T) {
	e := NewExchange()

	c := newTestConsumer(e)
	topic := Topic("test")

	e.Subscribe(topic, c)

	e.Publish(topic, []byte("testdata"))

	gotData := <-c.gotMsgs
	if !bytes.Equal(gotData.Data, []byte("testdata")) {
		t.Errorf("consumer got unexpected data: want 'testdata' got %s", string(gotData.Data))
	}
}

func TestSeveralMessages(t *testing.T) {
	topic := Topic("test")
	e := NewExchange()

	c := newTestConsumer(e)

	e.Subscribe(topic, c)

	for i := 0; i < 30; i++ {
		payloadBytes := append([]byte("testdata"), intToBytes(i)...)
		e.Publish(topic, payloadBytes)
	}

	for i := 0; i < 30; i++ {
		gotData := <-c.gotMsgs
		want := append([]byte("testdata"), intToBytes(i)...)
		if !bytes.Equal(gotData.Data, want) {
			t.Errorf("consumer got unexpected data: want '%s' got '%s'", string(want), string(gotData.Data))
		}
	}
}

func TestExchangeWithProducer(t *testing.T) {
	topic := Topic("test")
	e := NewExchange()

	c := newTestConsumer(e)
	p := PayloadProducer{e: e}

	e.Subscribe(topic, c)

	for i := 0; i < 30; i++ {
		payloadBytes := append([]byte("testdata"), intToBytes(i)...)
		p.Publish(topic, payloadBytes)
	}

	for i := 0; i < 30; i++ {
		gotData := <-c.gotMsgs
		want := append([]byte("testdata"), intToBytes(i)...)
		if !bytes.Equal(gotData.Data, want) {
			t.Errorf("consumer got unexpected data: want '%s' got '%s'", string(want), string(gotData.Data))
		}
	}
}

func TestExchangeStopWorks(t *testing.T) {
	topic := Topic("test")
	e := NewExchange()

	c := newTestConsumer(e)

	e.Subscribe(topic, c)

	e.Publish(topic, []byte("testdata"))
	e.Stop()
	e.Publish(topic, []byte("testdata"))
}

func TestExchangeSubscibeSecondCosnumerShouldNotWork(t *testing.T) {
	e := NewExchange()

	c := newTestConsumer(e)
	c2 := newTestConsumer(e)
	go func() {
		<-c2.gotMsgs
		t.Errorf("receiving message should not happen")
	}()

	topic := Topic("test")

	e.Subscribe(topic, c)
	e.Subscribe(topic, c2)

	e.Publish(topic, []byte("testdata"))
	<-c.gotMsgs
}

func TestExchangeAcksMessage(t *testing.T) {
	e := NewExchange()

	c := newTestConsumer(e)
	topic := Topic("test")

	e.Subscribe(topic, c)
	e.Publish(topic, []byte("testdata"))

	gotMsg := <-c.gotMsgs
	if len(e.consumers[topic].unackedMsgs) != 1 {
		t.Error("wrong unacked messages count")
	}

	c.Ack(topic, gotMsg.Id)

	if len(e.consumers[topic].unackedMsgs) != 0 {
		t.Error("wrong unacked messages count")
	}
}

func TestExchangeWithGoroutinesDataRace(t *testing.T) {
	wg := sync.WaitGroup{}
	e := NewExchange()

	c := newTestConsumer(e)
	c1 := newTestConsumer(e)
	c2 := newTestConsumer(e)
	c3 := newTestConsumer(e)
	topic := Topic("test")
	topic1 := Topic("test")
	topic2 := Topic("test")
	topic3 := Topic("test")

	wg.Add(1)
	go func() {
		defer wg.Done()
		e.Subscribe(topic, c)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		e.Subscribe(topic1, c1)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		e.Subscribe(topic2, c2)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		e.Subscribe(topic3, c3)
	}()

	go e.Publish(topic, []byte("testdata"))
	go e.Publish(topic1, []byte("testdata"))
	go e.Publish(topic2, []byte("testdata"))
	go e.Publish(topic3, []byte("testdata"))

	wg.Wait()
}

func TestExchangeUnsubscibe(t *testing.T) {
	e := NewExchange()

	c := newTestConsumer(e)
	topic := Topic("test")

	e.Subscribe(topic, c)
	e.Publish(topic, []byte("testdata"))
	<-c.gotMsgs

	e.Unsubscribe(topic)
	e.Publish(topic, []byte("testdata1"))
	e.Publish(topic, []byte("testdata2"))
	e.Publish(topic, []byte("testdata3"))

	if e.consumers[topic].c.Load() != nil {
		t.Errorf("consumer must be nil after unsubscribe")
	}

	go func() {
		select {
		case <-c.gotMsgs:
			t.Error("should be blocked")
		default:
			return
		}
	}()

	e.Subscribe(topic, c)

	for i := 1; i <= 3; i++ {
		gotData := <-c.gotMsgs
		want := append([]byte("testdata"), intToBytes(i)...)
		if !bytes.Equal(gotData.Data, want) {
			t.Errorf("consumer got unexpected data: want '%s' got '%s'", string(want), string(gotData.Data))
		}
	}
}

package broker

import (
	"bytes"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func intToBytes(i int) []byte {
	return []byte(strconv.Itoa(i))
}

type testConsumer struct {
	id      uuid.UUID
	gotMsgs chan Message
	e       *Exchange
}

func newTestConsumer(id uuid.UUID, e *Exchange) *testConsumer {
	return &testConsumer{
		id:      id,
		gotMsgs: make(chan Message, 1),
		e:       e,
	}
}

func (c testConsumer) Chan() chan Message {
	return c.gotMsgs
}

func (c testConsumer) ID() uuid.UUID {
	return c.id
}

func TestBasicConsume(t *testing.T) {
	e := NewExchange()

	c := newTestConsumer(uuid.New(), e)
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

	c := newTestConsumer(uuid.New(), e)

	e.Subscribe(topic, c)

	go func() {
		for i := 0; i < 30; i++ {
			payloadBytes := append([]byte("testdata"), intToBytes(i)...)
			e.Publish(topic, payloadBytes)
		}
	}()

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

	c := newTestConsumer(uuid.New(), e)
	p := PayloadProducer{e: e}

	e.Subscribe(topic, c)

	go func() {
		for i := 0; i < 30; i++ {
			payloadBytes := append([]byte("testdata"), intToBytes(i)...)
			p.Publish(topic, payloadBytes)
		}
	}()

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

	c := newTestConsumer(uuid.New(), e)

	e.Subscribe(topic, c)

	e.Publish(topic, []byte("testdata"))
	e.Stop()
	e.Publish(topic, []byte("testdata"))
}

func TestExchangeSubscribeMultipleConsumers(t *testing.T) {
	e := NewExchange()

	c := newTestConsumer(uuid.New(), e)
	c2 := newTestConsumer(uuid.New(), e)

	topic := Topic("test")

	e.Subscribe(topic, c)
	e.Subscribe(topic, c2)

	e.Publish(topic, []byte("testdata"))

	t.Log("first msg:", <-c.Chan(), "second msg:", <-c2.Chan())
}

func TestExchangeWithGoroutinesDataRace(t *testing.T) {
	wg := sync.WaitGroup{}
	e := NewExchange()

	c := newTestConsumer(uuid.New(), e)
	c1 := newTestConsumer(uuid.New(), e)
	c2 := newTestConsumer(uuid.New(), e)
	c3 := newTestConsumer(uuid.New(), e)
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

	wg.Wait()

	go e.Publish(topic, []byte("testdata"))
	go e.Publish(topic1, []byte("testdata"))
	go e.Publish(topic2, []byte("testdata"))
	go e.Publish(topic3, []byte("testdata"))

	time.Sleep(1 * time.Second)

}

func TestExchangeUnsubscibe(t *testing.T) {
	e := NewExchange()

	c := newTestConsumer(uuid.New(), e)
	topic := Topic("test")

	e.Subscribe(topic, c)
	e.Publish(topic, []byte("testdata"))

	t.Log("GOT BEFORE UNSUBSCRIBE", <-c.Chan())

	e.Unsubscribe(topic, c)
	e.Publish(topic, []byte("testdata1"))
	e.Publish(topic, []byte("testdata2"))
	e.Publish(topic, []byte("testdata3"))

	go func() {
		for range c.gotMsgs {
			t.Error("should be blocked but got message instead")
		}
	}()

	time.Sleep(1 * time.Second)
}

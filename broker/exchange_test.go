package broker

import "testing"

func TestBasicConsume(t *testing.T) {
	c := PayloadConsumer{}
	topic := Topic("test")
	e := NewExchange()
	e.ListenForMessages()

	e.Subscribe(topic, &c)

	e.Publish(topic, Payload{[]byte("testdata")})
}

func TestSeveralMessages(t *testing.T) {
	c := PayloadConsumer{}
	topic := Topic("test")
	e := NewExchange()
	e.ListenForMessages()

	e.Subscribe(topic, &c)

	for i := 0; i < 30; i++ {
		e.Publish(topic, Payload{[]byte("testdata")})
	}
}

func TestExchangeWithProducer(t *testing.T) {
	c := PayloadConsumer{}
	topic := Topic("test")
	e := NewExchange()
	p := PayloadProducer{e: e}
	e.ListenForMessages()

	e.Subscribe(topic, &c)

	for i := 0; i < 30; i++ {
		p.Publish(topic, []byte("testdata"))
	}
}

func TestExchangeStopWorks(t *testing.T) {
	c := PayloadConsumer{}
	topic := Topic("test")
	e := NewExchange()
	e.ListenForMessages()

	e.Subscribe(topic, &c)

	e.Publish(topic, Payload{[]byte("testdata")})
	e.Stop()
	e.Publish(topic, Payload{[]byte("testdata")})
}

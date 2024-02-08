package broker

import "testing"

func TestBasicConsume(t *testing.T) {
	c := Consumer{}
	topic := Topic("test")
	e := NewExchange()
	e.ListenForMessages()

	e.Subscribe(topic, &c)

	e.Publish(topic, Payload{[]byte("testdata")})
}

func TestSeveralMessages(t *testing.T) {
	c := Consumer{}
	topic := Topic("test")
	e := NewExchange()
	e.ListenForMessages()

	e.Subscribe(topic, &c)

	for i := 0; i < 30; i++ {
		e.Publish(topic, Payload{[]byte("testdata")})
	}
}

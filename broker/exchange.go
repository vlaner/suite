package broker

import "sync"

type Topic string

type Exchange struct {
	consumers map[Topic]*Consumer
	msgsCh    chan Message
	wg        sync.WaitGroup
}

func NewExchange() *Exchange {
	return &Exchange{
		consumers: map[Topic]*Consumer{},
		msgsCh:    make(chan Message, 100),
		wg:        sync.WaitGroup{},
	}
}

func (e *Exchange) Subscribe(topic Topic, c *Consumer) {
	_, exists := e.consumers[topic]
	if !exists {
		e.consumers[topic] = c
	}
}

func (e *Exchange) Publish(topic Topic, payload Payload) {
	e.msgsCh <- Message{topic: topic, payload: payload}
}

func (e *Exchange) ProcessMessage(msg Message) {
	consumer, exists := e.consumers[msg.topic]
	if exists {
		consumer.Consume(msg.payload)
	}
}

func (e *Exchange) ListenForMessages() {
	go func() {
		for message := range e.msgsCh {
			e.wg.Add(1)
			go func(msg Message) {
				defer e.wg.Done()
				e.ProcessMessage(msg)
			}(message)
		}
	}()

}

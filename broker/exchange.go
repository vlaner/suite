package broker

import (
	"sync"
)

type Topic string

type Exchange struct {
	consumers map[Topic]Consumer
	deadMsgs  chan Message
	msgsCh    chan Message
	wg        sync.WaitGroup
	sync      sync.Mutex
	quit      chan struct{}
}

func NewExchange() *Exchange {
	return &Exchange{
		consumers: map[Topic]Consumer{},
		deadMsgs:  make(chan Message, 100),
		msgsCh:    make(chan Message, 100),
		wg:        sync.WaitGroup{},
		quit:      make(chan struct{}),
	}
}

func (e *Exchange) Stop() {
	close(e.quit)
	close(e.msgsCh)
	close(e.deadMsgs)
	e.wg.Wait()
}

func (e *Exchange) Subscribe(topic Topic, c Consumer) {
	e.sync.Lock()
	defer e.sync.Unlock()

	_, exists := e.consumers[topic]
	if !exists {
		e.consumers[topic] = c
	}
}

func (e *Exchange) Publish(topic Topic, payload Payload) {
	select {
	case <-e.quit:
		return
	default:
		e.msgsCh <- Message{topic: topic, payload: payload}
	}
}

func (e *Exchange) ProcessMessage(msg Message) {
	select {
	case <-e.quit:
		return
	default:
		e.sync.Lock()
		defer e.sync.Unlock()

		consumer, exists := e.consumers[msg.topic]
		if !exists {
			e.deadMsgs <- msg
			return
		}

		consumer.Consume(msg.payload)
	}
}

func (e *Exchange) ListenForMessages() {
	go func() {
		for {
			select {
			case <-e.quit:
				return
			case msg := <-e.deadMsgs:
				e.wg.Add(1)
				e.ProcessMessage(msg)
				e.wg.Done()

			}
		}
	}()

	go func() {
		for {
			select {
			case <-e.quit:
				return
			case message := <-e.msgsCh:
				e.wg.Add(1)
				e.ProcessMessage(message)
				e.wg.Done()
			}
		}
	}()
}

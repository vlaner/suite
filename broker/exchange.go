package broker

import (
	"sync"
)

type Topic string

type Exchange struct {
	consumers map[Topic]Consumer
	msgsCh    chan Message
	wg        sync.WaitGroup
	sync      sync.RWMutex
	quit      chan struct{}
}

func NewExchange() *Exchange {
	return &Exchange{
		consumers: map[Topic]Consumer{},
		msgsCh:    make(chan Message, 100),
		wg:        sync.WaitGroup{},
		quit:      make(chan struct{}),
	}
}

func (e *Exchange) Stop() {
	close(e.quit)
	close(e.msgsCh)
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
	e.sync.RLock()
	defer e.sync.RUnlock()

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

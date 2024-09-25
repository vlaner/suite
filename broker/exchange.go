package broker

import (
	"sync"

	"github.com/google/uuid"
)

type Consumer interface {
	ID() uuid.UUID
	Chan() chan Message
}

type Topic string

type Exchange struct {
	consumers map[Topic][]Consumer
	wg        sync.WaitGroup
	sync      sync.Mutex
	quit      chan struct{}
}

func NewExchange() *Exchange {
	return &Exchange{
		consumers: make(map[Topic][]Consumer),
		wg:        sync.WaitGroup{},
		quit:      make(chan struct{}),
	}
}

func (e *Exchange) Stop() {
	close(e.quit)
	e.wg.Wait()
}

func (e *Exchange) Subscribe(topic Topic, c Consumer) {
	e.sync.Lock()
	defer e.sync.Unlock()

	_, exists := e.consumers[topic]
	if !exists {
		e.consumers[topic] = []Consumer{}
	}

	e.consumers[topic] = append(e.consumers[topic], c)
}

func (e *Exchange) Unsubscribe(topic Topic, c Consumer) {
	e.sync.Lock()
	defer e.sync.Unlock()

	consumers, exists := e.consumers[topic]
	if !exists {
		return
	}

	for i, cons := range consumers {
		if c.ID() == cons.ID() {
			consumers = append(consumers[:i], consumers[i+1:]...)
			break
		}
	}

	e.consumers[topic] = consumers
}

func (e *Exchange) Publish(topic Topic, data []byte) {
	e.sync.Lock()
	defer e.sync.Unlock()
	select {
	case <-e.quit:
		return
	default:
		msg := newMessage(uuid.New(), topic, data)
		_, exists := e.consumers[topic]
		if !exists {
			return
		}

		for _, consumer := range e.consumers[topic] {
			consumer.Chan() <- msg
		}
	}
}

package broker

import (
	"sync"
	"time"
)

type Topic string

type Exchange struct {
	consumers map[Topic]Consumer
	deadMsgs  []Message
	msgsCh    chan Message
	wg        sync.WaitGroup
	sync      sync.Mutex
	quit      chan struct{}
}

func NewExchange() *Exchange {
	return &Exchange{
		consumers: map[Topic]Consumer{},
		deadMsgs:  []Message{},
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
	e.sync.Lock()
	defer e.sync.Unlock()

	consumer, exists := e.consumers[msg.topic]
	if !exists {
		e.deadMsgs = append(e.deadMsgs, msg)
		return
	}

	consumer.Consume(msg.payload)
}

func (e *Exchange) ListenForMessages() {
	go func() {
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-e.quit:
				return
			case <-ticker.C:
				for _, msg := range e.deadMsgs {
					e.wg.Add(1)
					go func(msg Message) {
						defer e.wg.Done()
						e.ProcessMessage(msg)
					}(msg)
				}
				e.deadMsgs = []Message{}
			}
		}
	}()

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

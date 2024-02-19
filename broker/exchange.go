package broker

import (
	"log"
	"sync"
)

type ConsumerQueue struct {
	c    Consumer
	q    *Queue
	quit chan struct{}
	mu   sync.Mutex
}

func (cq *ConsumerQueue) listenForMessages() {
	for {
		select {
		case <-cq.quit:
			return
		default:
			message := cq.q.Dequeue()
			cq.processMessage(*message)
		}
	}
}

func (cq *ConsumerQueue) processMessage(msg Message) {
	cq.mu.Lock()
	defer cq.mu.Unlock()

	select {
	case <-cq.quit:
		return
	default:
		// TODO: convert to signal -> when consumer is added/removed
		if cq.c == nil {
			log.Println("consumer is nil")
			cq.q.EnqueueFront(&msg)
			return
		}

		err := cq.c.Consume(msg.payload)
		if err != nil {
			cq.q.EnqueueFront(&msg)
			log.Println("consumer error:", err)
		}
	}
}

func NewConsumerQueue(c Consumer) *ConsumerQueue {
	cq := ConsumerQueue{
		c:    c,
		q:    NewQueue(),
		quit: make(chan struct{}),
		mu:   sync.Mutex{},
	}

	go cq.listenForMessages()

	return &cq
}

type Topic string

type Exchange struct {
	consumers map[Topic]*ConsumerQueue
	wg        sync.WaitGroup
	sync      sync.Mutex
	quit      chan struct{}
}

func NewExchange() *Exchange {
	return &Exchange{
		consumers: map[Topic]*ConsumerQueue{},
		wg:        sync.WaitGroup{},
		quit:      make(chan struct{}),
	}
}

func (e *Exchange) Stop() {
	close(e.quit)
	e.sync.Lock()
	defer e.sync.Unlock()

	for _, c := range e.consumers {
		c.quit <- struct{}{}
	}

	e.wg.Wait()
}

func (e *Exchange) Subscribe(topic Topic, c Consumer) {
	e.sync.Lock()
	defer e.sync.Unlock()

	cons, exists := e.consumers[topic]
	if !exists {
		cq := NewConsumerQueue(c)
		e.consumers[topic] = cq
		return
	}

	cons.c = c
	e.consumers[topic] = cons
}

func (e *Exchange) Publish(topic Topic, payload Payload) {
	e.sync.Lock()
	defer e.sync.Unlock()

	select {
	case <-e.quit:
		return
	default:
		c, exists := e.consumers[topic]
		if !exists {
			cq := NewConsumerQueue(nil)
			e.consumers[topic] = cq
			cq.q.Enqueue(&Message{topic: topic, payload: payload})
			return
		}

		c.q.Enqueue(&Message{topic: topic, payload: payload})
	}
}

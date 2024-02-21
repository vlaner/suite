package broker

import (
	"log"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
)

type ConsumerQueue struct {
	// c - 'Consumer' interface
	c           atomic.Value
	q           *Queue
	unackedMsgs []Message
	quit        chan struct{}
	mu          *sync.Mutex
	muMsg       *sync.Mutex
	signal      *sync.Cond
}

func (cq *ConsumerQueue) listenForMessages() {
	for {
		select {
		case <-cq.quit:
			return
		default:
			cq.signal.L.Lock()

			for cq.c.Load() == nil {
				cq.signal.Wait()
			}

			message := cq.q.Dequeue()

			cq.muMsg.Lock()
			cq.unackedMsgs = append(cq.unackedMsgs, *message)
			cq.muMsg.Unlock()

			cq.processMessage(*message)

			cq.signal.L.Unlock()
		}
	}
}

func (cq *ConsumerQueue) processMessage(msg Message) {
	select {
	case <-cq.quit:
		return
	default:
		err := (cq.c.Load().(Consumer)).Consume(msg)
		if err != nil {
			cq.q.EnqueueFront(&msg)
			log.Println("consumer error:", err)
		}
	}
}

func (cq *ConsumerQueue) ack(msgId uuid.UUID) {
	cq.muMsg.Lock()
	defer cq.muMsg.Unlock()

	for i, msg := range cq.unackedMsgs {
		if msg.Id == msgId {
			cq.unackedMsgs = append(cq.unackedMsgs[:i], cq.unackedMsgs[i+1:]...)
		}
	}
}

func NewConsumerQueue(c Consumer) *ConsumerQueue {
	cq := ConsumerQueue{
		c:      atomic.Value{},
		q:      NewQueue(),
		quit:   make(chan struct{}),
		mu:     &sync.Mutex{},
		muMsg:  &sync.Mutex{},
		signal: sync.NewCond(&sync.Mutex{}),
	}

	if c != nil {
		cq.c.Store(c)
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
		cq.signal.Broadcast()
		return
	}

	if cons.c.Load() == nil {
		cons.c.Store(c)
		e.consumers[topic] = cons
		cons.signal.Broadcast()
	}
}

func (e *Exchange) Publish(topic Topic, data []byte) {
	e.sync.Lock()
	defer e.sync.Unlock()

	select {
	case <-e.quit:
		return
	default:
		message := newMessage(uuid.New(), topic, data)

		c, exists := e.consumers[topic]
		if !exists {
			cq := NewConsumerQueue(nil)
			e.consumers[topic] = cq
			cq.q.Enqueue(message)
			return
		}

		c.q.Enqueue(message)
	}
}

func (e *Exchange) Ack(topic Topic, msgId uuid.UUID) {
	e.sync.Lock()
	defer e.sync.Unlock()

	select {
	case <-e.quit:
		return
	default:
		c := e.consumers[topic]
		if c != nil {
			c.ack(msgId)
		}
	}
}

func (e *Exchange) GetUnackedMessages(topic Topic) []Message {
	e.sync.Lock()
	defer e.sync.Unlock()

	c := e.consumers[topic]
	return c.unackedMsgs
}

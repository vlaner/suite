package broker

import (
	"log"
	"sync"

	"github.com/google/uuid"
)

type ConsumerQueue struct {
	c           Consumer
	q           *Queue
	unackedMsgs []Message
	quit        chan struct{}
	mu          *sync.Mutex
	signal      *sync.Cond
}

func (cq *ConsumerQueue) listenForMessages() {
	for {
		select {
		case <-cq.quit:
			return
		default:
			cq.mu.Lock()

			for cq.c == nil {
				cq.signal.Wait()
			}

			message := cq.q.Dequeue()
			cq.unackedMsgs = append(cq.unackedMsgs, *message)
			cq.processMessage(*message)

			cq.mu.Unlock()
		}
	}
}

func (cq *ConsumerQueue) processMessage(msg Message) {
	select {
	case <-cq.quit:
		return
	default:

		err := cq.c.Consume(msg)
		if err != nil {
			cq.q.EnqueueFront(&msg)
			log.Println("consumer error:", err)
		}
	}
}

func (cq *ConsumerQueue) ack(msgId uuid.UUID) {
	for i, msg := range cq.unackedMsgs {
		if msg.Id == msgId {
			cq.unackedMsgs = append(cq.unackedMsgs[:i], cq.unackedMsgs[i+1:]...)
		}
	}
}

func NewConsumerQueue(c Consumer) *ConsumerQueue {
	mu := &sync.Mutex{}
	cq := ConsumerQueue{
		c:      c,
		q:      NewQueue(),
		quit:   make(chan struct{}),
		mu:     mu,
		signal: sync.NewCond(mu),
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
	if cons.c == nil {
		cons.c = c
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
	c := e.consumers[topic]
	return c.unackedMsgs
}

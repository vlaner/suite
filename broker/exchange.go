package broker

import (
	"log"
	"sync"
)

type Topic string

type Exchange struct {
	consumers map[Topic]Consumer
	wg        sync.WaitGroup
	sync      sync.Mutex
	quit      chan struct{}
	msgsQueue *Queue
}

func NewExchange() *Exchange {
	return &Exchange{
		consumers: map[Topic]Consumer{},
		wg:        sync.WaitGroup{},
		quit:      make(chan struct{}),
		msgsQueue: NewQueue(),
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
		e.consumers[topic] = c
	}
}

func (e *Exchange) Publish(topic Topic, payload Payload) {
	select {
	case <-e.quit:
		return
	default:
		e.msgsQueue.Enqueue(&Message{topic: topic, payload: payload})
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
			e.msgsQueue.EnqueueFront(&msg)
			return
		}

		err := consumer.Consume(msg.payload)
		if err != nil {
			log.Println("consumer error:", err)
			// e.Publish(msg.topic, msg.payload)
		}
	}
}

func (e *Exchange) ListenForMessages() {
	go func() {
		for {
			select {
			case <-e.quit:
				return
			default:
				e.wg.Add(1)
				message := e.msgsQueue.Dequeue()
				e.ProcessMessage(*message)
				e.wg.Done()
			}
		}
	}()
}

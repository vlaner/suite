package broker

import (
	"sync"
)

type Queue struct {
	msgs   []*Message
	sync   *sync.Mutex
	signal *sync.Cond
}

func NewQueue() *Queue {
	mu := &sync.Mutex{}
	return &Queue{
		msgs:   []*Message{},
		sync:   mu,
		signal: sync.NewCond(mu),
	}
}

func (q *Queue) Enqueue(msg *Message) {
	q.sync.Lock()
	defer q.sync.Unlock()

	q.msgs = append(q.msgs, msg)
	q.signal.Broadcast()
}

func (q *Queue) Dequeue() *Message {
	q.sync.Lock()
	defer q.sync.Unlock()

	for {
		for len(q.msgs) == 0 {
			q.signal.Wait()
		}
		msg := q.msgs[0]
		q.msgs = q.msgs[1:]
		return msg
	}

}

func (q *Queue) EnqueueFront(msg *Message) {
	q.sync.Lock()
	defer q.sync.Unlock()
	q.msgs = append([]*Message{msg}, q.msgs...)
	q.signal.Broadcast()
}

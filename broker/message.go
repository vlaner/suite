package broker

import "github.com/google/uuid"

type Message struct {
	Id    uuid.UUID
	Topic Topic
	Data  []byte
}

func newMessage(id uuid.UUID, topic Topic, data []byte) *Message {
	return &Message{
		Id:    id,
		Topic: topic,
		Data:  data,
	}
}

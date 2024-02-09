package broker

type Producer struct {
	e *Exchange
}

func (p *Producer) Publish(topic Topic, data []byte) {
	p.e.Publish(topic, Payload{Data: data})
}

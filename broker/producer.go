package broker

type Producer interface {
	Publish(topic Topic, data []byte)
}

type PayloadProducer struct {
	e *Exchange
}

func (p *PayloadProducer) Publish(topic Topic, data []byte) {
	p.e.Publish(topic, Payload{Data: data})
}

package broker

type Consumer interface {
	Consume(Message) error
}

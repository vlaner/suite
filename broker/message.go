package broker

type Message struct {
	topic   Topic
	payload Payload
}

type Payload struct {
	Data []byte
}

package kafka

// New to create new client
func New(o ...Option) *Message {
	opts := newOptions(o...)
	return &Message{
		options: opts,
	}
}

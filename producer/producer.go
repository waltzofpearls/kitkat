package producer

type Producer struct {
	Stream string
	Region string
}

func New() *Producer {
	return new(Producer)
}

func (p *Producer) Write() {
}

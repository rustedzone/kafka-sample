package consumer

// Repository function contract for customer repo
type Repository interface {
	// consume message from broker within given topic
	// will loop forever
	Consume(topic string)
}

package kafkaelasticsearch

import "context"

// Consume consumer streaming data from kafka store it to es
// will loop forever
func (repo *ConsumerRepo) Consume(topic string) {
	for {
		repo.consumerGroup.Consume(context.Background(), []string{topic}, repo)
	}
}

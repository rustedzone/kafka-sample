package commandline

import (
	uConsumeKafka "github.com/rustedzone/kafka-sample/internal/usecase/consume_kafka"
)

func newUsecaseConsumeKafka(f flagAttr) uConsumeKafka.Usecase {
	repoConsumerKafkaES := newRepoConsumer(f)
	return uConsumeKafka.NewUseCase(
		repoConsumerKafkaES,
	)
}

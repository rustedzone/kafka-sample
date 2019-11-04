package commandline

import (
	uConsumeKafka "github.com/rustedzone/kafka-sample/internal/usecase/consume_kafka"
)

func newUsecaseConsumeKafka(f flagAttr) uConsumeKafka.Usecase {
	repoConsumerKafkaES := fnNewRepoConsumer(f)
	return fnConsumeKafkaNewUsecase(
		repoConsumerKafkaES,
	)
}

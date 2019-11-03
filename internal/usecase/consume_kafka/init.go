package consumekafkaproduct

import (
	"log"

	rConsumer "github.com/rustedzone/kafka-sample/internal/repository/consumer"
)

// NewUseCase : create consume kafka product usecase instance
func NewUseCase(rConsumerKafkaES rConsumer.Repository) Usecase {
	repo := repoAttr{
		consumerKafkaES: rConsumerKafkaES,
	}
	if !checkRepo(repo) {
		log.Panicf("[consumeKafkaProduct][NewUseCase] missing repo %+v", repo)
	}

	return &UseCase{
		repo: repo,
	}
}

func checkRepo(repo repoAttr) bool {
	if repo.consumerKafkaES == nil {
		return false
	}
	return true
}

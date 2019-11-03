package consumekafkaproduct

import (
	rConsumer "github.com/rustedzone/kafka-sample/internal/repository/consumer"
)

type (

	// UseCase : kafaka elastic search's UseCase instance
	UseCase struct {
		repo repoAttr
	}

	repoAttr struct {
		consumerKafkaES rConsumer.Repository
	}
)

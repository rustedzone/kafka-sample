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

	// UsecaseMock need to be separated
	UsecaseMock struct {
		// ConsumeAndStoreToESFunc mocks the ConsumeAndStoreToES method.
		ConsumeAndStoreToESFunc func(topic string)

		// calls tracks calls to the methods.
		calls struct {
			// ConsumeAndStoreToES holds details about calls to the ConsumeAndStoreToES method.
			ConsumeAndStoreToES []struct {
				// Topic is the topic argument value.
				Topic string
			}
		}
	}
)

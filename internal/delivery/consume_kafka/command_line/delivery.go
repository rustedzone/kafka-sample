package commandline

import (
	"errors"
	"log"

	"flag"

	rConsumer "github.com/rustedzone/kafka-sample/internal/repository/consumer"
	rConsumerKafkaES "github.com/rustedzone/kafka-sample/internal/repository/consumer/kafka_elasticsearch"
	uConsumeKafka "github.com/rustedzone/kafka-sample/internal/usecase/consume_kafka"
	elastic "gopkg.in/olivere/elastic.v6"
)

var (
	f flagAttr

	// mocked function, unit test purpose
	fnConsumerKafkaESNewRepo func(opt rConsumerKafkaES.InitOption) rConsumer.Repository
	fnElasticNewClient       func(opt ...elastic.ClientOptionFunc) (*elastic.Client, error)
	fnConsumeKafkaNewUsecase func(rConsumer.Repository) uConsumeKafka.Usecase
	fnNewRepoConsumer        func(f flagAttr) rConsumer.Repository
	fnNewUsecaseConsumeKafka func(f flagAttr) uConsumeKafka.Usecase
)

func init() {
	flag.StringVar(&f.broker, "kafka-broker", "", "kafka broker host")
	flag.StringVar(&f.groupName, "kafka-group-name", "", "targeted kafka group name ")
	flag.StringVar(&f.topic, "kafka-topic", "", "targeted kafka topic")
	flag.BoolVar(&f.saramalog, "enable-log-sarama", false, "print out sarama log")
	flag.StringVar(&f.esHost, "es-host", "", "elastic search host")
	flag.StringVar(&f.esIndex, "es-index", "", "targeted index of elastic search")
	flag.StringVar(&f.esType, "es-type", "", "targeted type of elastic search")

	// mock Imported function
	fnConsumerKafkaESNewRepo = rConsumerKafkaES.NewRepoConsumer
	fnElasticNewClient = elastic.NewClient
	fnConsumeKafkaNewUsecase = uConsumeKafka.NewUseCase

	// mock internal function
	fnNewRepoConsumer = newRepoConsumer
	fnNewUsecaseConsumeKafka = newUsecaseConsumeKafka
}

// Deliver : deliver kafka consumer through command-line
func Deliver() {
	if err := validateFlag(f); err != nil {
		log.Printf("[consume-kafka]Deliver:invalid flag : %+v", err)
		return
	}
	fnNewUsecaseConsumeKafka(f).ConsumeAndStoreToES(f.topic)
}

func validateFlag(f flagAttr) error {
	if f.broker == "" {
		return errors.New("missing kafka broker address")
	}
	if f.groupName == "" {
		return errors.New("missing kafka's group name")
	}
	if f.topic == "" {
		return errors.New("missing kafka's topic")
	}
	if f.esHost == "" {
		return errors.New("missing elasticsearch host")
	}
	if f.esIndex == "" {
		return errors.New("missing elasticsearch index")
	}
	if f.esType == "" {
		return errors.New("missing elasticsearch index's type")
	}
	return nil
}

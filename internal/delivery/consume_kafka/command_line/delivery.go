package commandline

import (
	"errors"
	"log"

	"flag"
)

var f flagAttr

func init() {
	flag.StringVar(&f.broker, "kafka-broker", "", "kafka broker host")
	flag.StringVar(&f.groupName, "kafka-group-name", "", "targeted kafka group name ")
	flag.StringVar(&f.topic, "kafka-topic", "", "targeted kafka topic")
	flag.BoolVar(&f.saramalog, "enable-log-sarama", false, "print out sarama log")
	flag.StringVar(&f.esHost, "es-host", "", "elastic search host")
	flag.StringVar(&f.esIndex, "es-index", "", "targeted index of elastic search")
	flag.StringVar(&f.esType, "es-type", "", "targeted type of elastic search")
}

// Deliver : init new object of  consume kafka  delivery
func Deliver() {
	if err := validateFlag(f); err != nil {
		log.Printf("[consume-kafka]Deliver:invalid flag : %+v", err)
	} else {
		newUsecaseConsumeKafka(f).ConsumeAndStoreToES(f.topic)
	}
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

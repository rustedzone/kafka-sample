package kafkaelasticsearch

import (
	"encoding/json"
	"errors"
	"log"

	jsoniter "github.com/json-iterator/go"
	"github.com/rustedzone/sample-kafka/common"
	mBroker "github.com/rustedzone/sample-kafka/internal/model/broker"
)

func prepareDoc(msg []byte) (doc json.RawMessage, action, docID string, err error) {
	message := mBroker.Message{}
	json := jsoniter.Config{
		EscapeHTML:                    false,
		ObjectFieldMustBeSimpleString: true, // do not unescape object field
	}.Froze()
	if err = json.Unmarshal(msg, &message); err != nil {
		log.Println("[prepareDoc] unmarshall:", err)
		return
	}
	if err = checkHeader(message.Header); err != nil {
		log.Println("[prepareDoc] check header:", err)
		return
	}

	doc = message.Body
	action = message.Header.Action
	docID = message.Header.Key
	return
}

func checkHeader(header mBroker.Header) (err error) {
	if header.Key == "" {
		return errors.New("ID is empty")
	}
	if action := header.Action; action != common.ActionIndex &&
		action != common.ActionCreate &&
		action != common.ActionUpdate &&
		action != common.ActionDelete {
		return errors.New("invalid action")
	}
	return
}

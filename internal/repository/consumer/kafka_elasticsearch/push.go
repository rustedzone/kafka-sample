package kafkaelasticsearch

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	elastic "gopkg.in/olivere/elastic.v6"
)

// pushBulk : push predefined bulk request to elasticsearch
// will do infinite retry if get expected error
// will panic if get unexpected error
func (repo *ConsumerRepo) pushBulk(bulkService *elastic.BulkService, countMessage int) (err error) {
	for {
		if err := repo.fn.bulkDo(bulkService, countMessage); err != nil {
			if !elastic.IsConnErr(err) {
				log.Panicf("[consumeBulk] FOUND NEW ERROR : %v", err)
			}
			time.Sleep(1 * time.Second)
		} else {
			break
		}
	}
	return
}

// BulkDo hit the elastic
func (repo *ConsumerRepo) bulkDo(bulk *elastic.BulkService, lenAction int) error {
	// Estimated size in bytes of bulk request & number of action
	bytes := bulk.EstimatedSizeInBytes()
	numberOfActions := bulk.NumberOfActions()

	if numberOfActions == 0 || bytes == 0 {
		return nil
	}

	// Do sends the bulk request
	estimatedBytes := bulk.EstimatedSizeInBytes()
	start := time.Now()
	resp, err := bulk.Do(context.Background())
	log.Println("Pushing to Elastic. index:", repo.es.index, "total byte:", estimatedBytes, "total data:", lenAction, "done in", time.Since(start).Seconds(), "seconds")
	if err != nil {
		return err
	}

	// Check failed
	failed := resp.Failed()
	if len(failed) > 0 {
		validFail := 0
		for _, response := range failed {
			if response.Status != http.StatusNotFound {

				// ignore error for document is missing  and document existed
				if response.Error.Type != errTypeDocumentMissingException && response.Error.CausedBy["reason"] != nil {
					if response.Error.CausedBy["reason"].(string) != errReasonDocumentExist {
						log.Println("reason: ", response.Error.Reason, ",", response.Error.CausedBy["reason"])
						validFail++
					}
				}
			}
		}
		if validFail > 0 {
			return fmt.Errorf("bulk index request %s failed: %d", repo.es.index, validFail)
		}
	}

	return nil
}

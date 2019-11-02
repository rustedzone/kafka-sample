package kafkaelasticsearch

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	elasticV6 "gopkg.in/olivere/elastic.v6"
)

// pushBulk : push predefined bulk request to elasticsearch
// will do infinite retry if get expected error
// will panic if get unexpected error
func (repo *ConsumerRepo) pushBulk(bulkService *elasticV6.BulkService, countMessage int) (err error) {
	for {
		if err := repo.fn.bulkDo(bulkService, countMessage); err != nil {
			if !elasticV6.IsConnErr(err) {
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
func (repo *ConsumerRepo) bulkDo(bulk *elasticV6.BulkService, lenAction int) error {
	// Estimated size in bytes of bulk request & number of action
	bytes := bulk.EstimatedSizeInBytes()
	numberOfActions := bulk.NumberOfActions()

	if numberOfActions == 0 || bytes == 0 {
		return nil
	}

	// Do sends the bulk request
	start := time.Now()
	resp, err := bulk.Do(context.Background())
	log.Println("Pushing to Elastic. index:", repo.es.index, "total byte:", bulk.EstimatedSizeInBytes(), "total data:", bulk.NumberOfActions(), "done in", time.Since(start).Seconds())
	if err != nil {
		return err
	}

	// Check failed
	failed := resp.Failed()
	if len(failed) > 0 {
		validFail := 0
		for _, response := range failed {
			if response.Status != http.StatusNotFound {

				// ignore document is missing  error and document existed error
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

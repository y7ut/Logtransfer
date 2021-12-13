package entity

import (
	"context"
	"log"
	"regexp"
	"sync"
	"time"

	elastic "github.com/olivere/elastic/v7"
	"github.com/y7ut/logtransfer/conf"
)

var (
	contentRegexp       = regexp.MustCompile(`\[(?s:(.*?))\]`)
	serviceWfLogKeyWord = []string{"errno", "logId", "uri", "refer", "cookie", "ua", "host", "clientIp", "optime", "request_params", "errmsg"}
	MatePool            = sync.Pool{New: func() interface{} { return &Matedata{Data: make(map[string]interface{})} }}
	messages            = make(chan *Matedata, conf.APPConfig.Es.BulkSize)
)

type Matedata struct {
	Topic  string
	Index  string
	Level  string
	create time.Time
	Data   map[string]interface{}
}

func (m *Matedata) reset() {
	m.Topic = ""
	m.Index = ""
	m.Level = ""
	m.Data = map[string]interface{}{}
}

func HandleMessage(m *Matedata) {
	messages <- m
}

func CloseMessageChan() {
	close(messages)
}

func MatedateSender(ctx context.Context, esClient *elastic.Client) {

	tick := time.NewTicker(3 * time.Second)
	var (
		SenderMu sync.Mutex
	)

	bulkRequest := esClient.Bulk()
	for {
		select {
		case m := <-messages:

			indexRequest := elastic.NewBulkIndexRequest().Index(m.Index).Doc(m.Data)
			bulkRequest.Add(indexRequest)

		case <-tick.C:
			// Do sends the bulk requests to Elasticsearch
			SenderMu.Lock()
			count := bulkRequest.NumberOfActions()

			if count > 0 {
				log.Printf("Send message to Es: %d : \n", bulkRequest.NumberOfActions())
				_, err := bulkRequest.Do(ctx)
				if err != nil {
					log.Println("Save Es Error:", err)
				}
				bulkRequest.Reset()
			}
			SenderMu.Unlock()

		case <-ctx.Done():
			// Do sends the bulk requests to Elasticsearch
			SenderMu.Lock()
			_, err := bulkRequest.Do(ctx)
			if err != nil {
				log.Println("Save Es Error:", err)
			}
			bulkRequest.Reset()
			SenderMu.Unlock()
			log.Println("Exiting...")
			return
		}
	}
}

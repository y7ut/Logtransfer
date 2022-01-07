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

func MatedateSender(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// 初始化ES客户端
	esClient, err := elastic.NewClient(elastic.SetSniff(false), elastic.SetURL(conf.APPConfig.Es.Address))
	if err != nil {
		panic(err)
	}

	wp := &ESWorkPool{
		WorkerFunc: func(matedatas []*Matedata) bool {
			bulkRequest := esClient.Bulk()
			for _, m := range matedatas {
				indexRequest := elastic.NewBulkIndexRequest().Index(m.Index).Doc(m.Data)
				bulkRequest.Add(indexRequest)
			}
			count := bulkRequest.NumberOfActions()
			if count > 0 {
				log.Printf("Send messages to Index: %d : \n", bulkRequest.NumberOfActions())
				timectx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
				response, err := bulkRequest.Do(timectx)
				cancel()
				if err != nil {
					log.Println("Save Es Error:", err)
					return false
				}

				for _, v := range response.Items {
					for _, item := range v {
						if item.Error != nil {
							log.Printf("Find Error in ES Result in (%s): %s", item.Index, item.Error.Reason)
							return false
						}
					}
				}

				bulkRequest.Reset()
			}
			return true
		},
		MaxWorkerCount:        50,
		MaxIdleWorkerDuration: 5 * time.Second,
	}

	wp.Start()
	defer wp.Stop()
	var mateDatesItems []*Matedata

	var mu sync.Mutex
	autoTicker := time.NewTicker(10 * time.Second)
	for {
		select {
		case m := <-messages:
			mu.Lock()
			mateDatesItems = append(mateDatesItems, m)
			currentItems := mateDatesItems

			if len(currentItems) > 10 {
				mateDatesItems = mateDatesItems[:0]
				mu.Unlock()
				wp.Serve(currentItems)
			}else{
				mu.Unlock()
			}
		case <-autoTicker.C:
			mu.Lock()
			currentItems := mateDatesItems
			
			if len(currentItems) > 0 {
				mateDatesItems = mateDatesItems[:0]
				mu.Unlock()
				wp.Serve(currentItems)
			}else{
				mu.Unlock()
			}

		case <-ctx.Done():

			log.Println("Exiting...")

			mu.Lock()
			currentItems := mateDatesItems
			mu.Unlock()

			wp.Serve(currentItems)
			return
		}
	}
}

package transfer

import (
	"encoding/json"
	"log"

	"github.com/y7ut/logtransfer/conf"
)

type Collector struct {
	Style string `json:"style"`
	Path  string `json:"path"`
	Topic string `json:"topic"`
}

// 加载所有的collector
func loadCollectors() []Collector {
	configs := conf.GetAllConfFromEtcd()
	collectors := make([]Collector, 0)
	for _, v := range configs {
		
		var currentCollector []Collector
		err := json.Unmarshal(v, &currentCollector)
		if err != nil {
			log.Printf("json decode config(%s) err :  err: %s", v, err)
		}
		if currentCollector !=nil {
			log.Printf("Init config:%s ", v)
			collectors = append(collectors, currentCollector...)
		}
	}
	return collectors
}

// 收集所有需要监听的topic
func ChooseTopic() map[string]bool {
	collector := loadCollectors()
	topics := make(map[string]bool, 0)
	for _, v := range collector {
		topics[v.Topic] = true
	}
	return topics
}

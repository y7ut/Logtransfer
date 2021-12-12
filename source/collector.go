package source

import (
	"encoding/json"
	"log"

	"github.com/y7ut/logtransfer/conf"
	"github.com/y7ut/logtransfer/entity"
	"github.com/y7ut/logtransfer/plugin"
)

type Collector struct {
	Style string `json:"style"`
	Path  string `json:"path"`
	Topic string `json:"topic"`
}

type Topic struct {
	Name     string
	Label    string
	PipeLine *plugin.PipeLine
	Format   entity.Formater
}

type TopicConfig struct {
	Format         int                     `json:"format"`
	Label          string                  `json:"label"`
	Name           string                  `json:"name"`
	PipelineConfig []PipeLinePluginsConfig `json:"piepline"`
}

type PipeLinePluginsConfig struct {
	Label  string `json:"label"`
	Name   string `json:"name"`
	Params string `json:"params"`
}

// 加载所有的collector
func LoadCollectors() []Collector {
	configs := conf.GetAllConfFromEtcd()
	collectors := make([]Collector, 0)
	for _, v := range configs {

		var currentCollector []Collector
		err := json.Unmarshal(v, &currentCollector)
		if err != nil {
			log.Printf("json decode config(%s) err :  err: %s", v, err)
		}
		if currentCollector != nil {
			log.Printf("Init config:%s ", v)
			collectors = append(collectors, currentCollector...)
		}
	}
	return collectors
}

// func loadTopics() map[string]*Topic {
// 	configs := conf.GetAllTopicFromEtcd()

// 	topics := make(map[string]*Topic)

// 	for _, v := range configs {

// 		var currentTopic TopicConfig
// 		err := json.Unmarshal(v, &currentTopic)
// 		if err != nil {
// 			log.Printf("json decode config(%s) err :  err: %s", v, err)
// 		}
// 		log.Printf("Init Topic:%s ", currentTopic.Label)
// 		if currentTopic.PipelineConfig == nil {
// 			log.Printf("get topic setting error:%s ", currentTopic.Label)
// 		}

// 		p := plugin.PipeLine{}

// 		// log.Println("get config",  currentTopic.PipelineConfig)
// 		for _, v := range currentTopic.PipelineConfig {
// 			currentPlugin := plugins.pluginsBoard[v.Name]
// 			err := currentPlugin.SetParams(v.Params)
// 			if err != nil {
// 				log.Panicln("plugin encode params error:", err)
// 			}
// 			p.AppendPlugin(currentPlugin)
// 		}
// 		var formatMethod entity.Formater

// 		switch currentTopic.Format {

// 		case 1:
// 			formatMethod = entity.DefaultJsonLog
// 		case 2:
// 			formatMethod = entity.FormatServiceWfLog
// 		default:
// 			formatMethod = entity.DefaultLog

// 		}
// 		topics[currentTopic.Name] = &Topic{Name: currentTopic.Name, Label: currentTopic.Label, PipeLine: &p, Format: formatMethod}
// 	}
// 	return topics
// }


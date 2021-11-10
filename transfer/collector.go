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

type Topic struct {
	Name     string
	Label    string
	PipeLine *PipeLine
	Format   Formater
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
func loadCollectors() []Collector {
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

func loadTopics() map[string]*Topic {
	configs := conf.GetAllTopicFromEtcd()

	topics := make(map[string]*Topic)

	for _, v := range configs {

		var currentTopic TopicConfig
		err := json.Unmarshal(v, &currentTopic)
		if err != nil {
			log.Printf("json decode config(%s) err :  err: %s", v, err)
		}
		log.Printf("Init Topic:%s ", currentTopic.Label)
		if currentTopic.PipelineConfig == nil {
			log.Printf("get topic setting error:%s ", currentTopic.Label)
		}

		p := PipeLine{}


		for _, v := range currentTopic.PipelineConfig {
			
		   makePiepline(&p,v)

		}
		var formatMethod Formater

		switch currentTopic.Format {

		case 1:
			formatMethod = DefaultJsonLog
		case 2:
			formatMethod = FormatServiceWfLog
		default:
			formatMethod = DefaultLog

		}
		topics[currentTopic.Name] = &Topic{Name: currentTopic.Name, Label: currentTopic.Label, PipeLine: &p, Format: formatMethod}
	}
	return topics
}

// 收集所有需要监听的topic
func ChooseTopic() map[*Topic]bool {
	collector := loadCollectors()
	topics := loadTopics()

	ableTopics := make(map[*Topic]bool)
	for _, v := range collector {
		currentTopic := topics[v.Topic]
		ableTopics[currentTopic] = true
	}

	return ableTopics
}

func makePiepline(p *PipeLine ,config PipeLinePluginsConfig){
	if p.Current == nil {
		current := pluginsBoard[config.Name]

		if config.Params != "" {
			current.setParams(config.Params)
		}

		p.Current = &current
		p.Next = &PipeLine{}
		log.Printf("install plugin :%s", config.Label,)
		return
	}
    makePiepline(p.Next, config)
}
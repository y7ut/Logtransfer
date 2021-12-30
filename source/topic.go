package source

import (
	"log"

	"github.com/y7ut/logtransfer/entity"
	"github.com/y7ut/logtransfer/plugin"
)

func generateTopic(config TopicConfig) *Topic {

	if config.PipelineConfig == nil {
		log.Printf("get topic setting error:%s ", config.Label)
	}

	p := plugin.PipeLine{}

	// log.Println("get config",  currentTopic.PipelineConfig)
	for _, v := range config.PipelineConfig {
		currentPlugin := plugin.RegistedPlugins[v.Name]
		err := currentPlugin.SetParams(v.Params)
		if err != nil {
			log.Panicln("plugin encode params error:", err)
		}
		p.AppendPlugin(currentPlugin)
	}
	var formatMethod entity.Formater

	switch config.Format {

	case 1:
		formatMethod = entity.DefaultJsonLog
	case 2:
		formatMethod = entity.FormatServiceWfLog
	default:
		formatMethod = entity.DefaultLog

	}

	return &Topic{Name: config.Name, Label: config.Label, PipeLine: &p, Format: formatMethod}
}

package plugin

import (
	"fmt"

	"github.com/y7ut/logtransfer/entity"
)

// 修改插件
type SaveES Plugin

func (saveEs *SaveES) HandleFunc(m *entity.Matedata) error {
	// log.Println("SaveES:")
	m.Index = fmt.Sprintf("%s", (*saveEs.params)["index"])
	m.Data["topic"] = m.Topic
	m.Data["level"] = m.Level
	entity.HandleMessage(m)
	return nil
}

func (saveEs *SaveES) SetParams(params string) error {

	paramsValue, err := checkParams(params, "index")
	if err != nil {
		return err
	}
	saveEs.params = &paramsValue
	return err
}

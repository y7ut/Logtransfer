package plugin

import (
	"fmt"
	"log"

	"github.com/y7ut/logtransfer/entity"
)

// 修改插件
type Edit Plugin

func (edit *Edit) HandleFunc(m *entity.Matedata) error {
	log.Println("EDIT:")
	if edit.params == nil {
		return fmt.Errorf("please set params first")
	}

	key := (*edit.params)["key"]
	value := (*edit.params)["value"]
	(*m).Data[key.(string)] = value
	return nil
}

func (edit *Edit) SetParams(params string) error {

	paramsValue, err := checkParams(params, "key", "value")
	if err != nil {
		return err
	}
	edit.params = &paramsValue
	return err
}

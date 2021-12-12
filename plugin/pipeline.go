package plugin

import (
	"encoding/json"
	"errors"
	"log"
	"strings"

	"github.com/y7ut/logtransfer/entity"
)


type Handler interface {
	HandleFunc(*entity.Matedata) error
	SetParams(string) error
}

type Plugin struct {
	params *map[string]interface{}
	// error  error
}

type PipeLine struct {
	pipe []*Handler
}

func (p *PipeLine) AppendPlugin(plugin Handler) {
	p.pipe = append(p.pipe, &plugin)
}

func (p *PipeLine) Enter(m entity.Matedata) {
	for _, plugin := range p.pipe {
		err := (*plugin).HandleFunc(&m)
		if err != nil {
			log.Println(err)
		}
	}
}

func checkParams(paramsJson string, key ...string) (value map[string]interface{}, err error) {
	err = json.Unmarshal([]byte(paramsJson), &value)

	if err != nil {
		return value, err
	}
	var errMessage strings.Builder
	var errCheck bool

	errMessage.WriteString("Plugin params errors: ")
	for _, checkItem := range key {
		if value[checkItem] == nil {
			errCheck = true
			errMessage.WriteString(checkItem)
		}
	}

	if errCheck {
		return value, errors.New(errMessage.String())
	}
	return value, nil
}

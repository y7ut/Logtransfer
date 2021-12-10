package transfer

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"
)

var pluginsBoard = map[string]Handler{
	"Dump":   &Dump{},
	"Edit":   &Edit{},
	"SaveES": &SaveES{},
}

type Matedate struct {
	Topic  string
	Index  string
	Level  string
	create time.Time
	data   map[string]interface{}
}

func (m *Matedate) reset() {
	m.Topic = ""
	m.Index = ""
	m.Level = ""
	m.data = map[string]interface{}{}
}

// type Matedate map[string]interface{}

type Handler interface {
	HandleFunc(*Matedate) error
	setParams(string) error
}

type Plugin struct {
	params *map[string]interface{}
	// error  error
}

type PipeLine struct {
	pipe []*Handler
}

func (p *PipeLine) appendPlugin(plugin Handler) {
	p.pipe = append(p.pipe, &plugin)
}

func (p *PipeLine) Enter(m Matedate) {
	for _, plugin := range p.pipe {
		err := (*plugin).HandleFunc(&m)
		if err != nil {
			log.Println(err)
		}
	}
}

// 打印插件
type Dump Plugin

func (dump *Dump) HandleFunc(m *Matedate) error {
	log.Println("DUMP:")
	for k, v := range (*m).data {
		fmt.Printf("%s : %s\n", k, v)
	}
	fmt.Println("------------")
	return nil
}

func (dump *Dump) setParams(params string) error {
	return nil
}

// 修改插件
type Edit Plugin

func (edit *Edit) HandleFunc(m *Matedate) error {
	log.Println("EDIT:")
	if edit.params == nil {
		return fmt.Errorf("please set params first")
	}

	key := (*edit.params)["key"]
	value := (*edit.params)["value"]
	(*m).data[key.(string)] = value
	return nil
}

func (edit *Edit) setParams(params string) error {

	paramsValue, err := checkParams(params, "key", "value")
	if err != nil{
		return err
	} 
	edit.params = &paramsValue
	return err
}

// 修改插件
type SaveES Plugin

func (saveEs *SaveES) HandleFunc(m *Matedate) error {
	log.Println("SaveES:")
	m.Index = fmt.Sprintf("%s", (*saveEs.params)["index"])
	m.data["topic"] = m.Topic
	m.data["level"] = m.Level
	messages <- m
	return nil
}

func (saveEs *SaveES) setParams(params string) error {

	paramsValue, err := checkParams(params, "index")
	if err != nil{
		return err
	} 
	saveEs.params = &paramsValue
	return err
}

// 警报与监测
type Alarm Plugin

func (alarm *Alarm) HandleFunc(m *Matedate) error {
	return nil
}

func (alarm *Alarm) setParams(params string) error {

	paramsValue, err := checkParams(params, "hit", "idle_time")
	if err != nil{
		return err
	} 
	alarm.params = &paramsValue

	return err
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

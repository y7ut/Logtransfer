package transfer

import (
	"encoding/json"
	"fmt"
	"log"
	"time"
)

var pluginsBoard = map[string]Handler{
	"Dump": &Dump{},
	"Edit": &Edit{},
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
	(*m).data["eidt"] = 1
	return nil
}

func (edit *Edit) setParams(params string) error {
	var paramsValue map[string]interface{}
	var checkKeyString bool
	var checkValueString bool

	err := json.Unmarshal([]byte(params), &paramsValue)

	for key := range paramsValue {
		if key == "key" {
			checkKeyString = true
		}
		if key == "value" {
			checkValueString = true
		}
	}

	if !(checkKeyString && checkValueString) {
		return fmt.Errorf("please set params true")
	}

	edit.params = &paramsValue
	return err
}

// 修改插件
type SaveES Plugin

func (saveEs *SaveES) HandleFunc(m *Matedate) error {
	log.Println("SaveES:")
	messages <- m
	return nil
}

func (saveEs *SaveES) setParams(params string) error {
	return nil
}

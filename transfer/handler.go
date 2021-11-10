package transfer

import (
	"encoding/json"
	"fmt"
	"log"
)

var pluginsBoard = map[string]Plugin{
	"Dump":&Dump{},
	"Edit":&Edit{},
	"SaveES":&SaveES{},
}

type Matedate map[string]interface{}

type Plugin interface {
	Handle(Matedate) Matedate
	setParams(string)
}

type PipeLine struct {
	Next    *PipeLine
	Current *Plugin
}

func (p PipeLine) Enter(m Matedate) Matedate {
	if p.Current == nil {
		return m
	}
	if p.Next == nil {
		return (*p.Current).Handle(m)
	}

	return p.Next.Enter((*p.Current).Handle(m))
}

// 修改插件
type Edit struct {
	Params map[string]interface{}
}

func (p Edit) Handle(m Matedate) Matedate {
	key := fmt.Sprintf("%s", p.Params["key"])
	log.Println(key)
	log.Println(p.Params["value"])
	m[key] = p.Params["value"]
	m["EDIT"] = "ok"
	return m
}

func (p *Edit) setParams(params string) {
	var paramsValue map[string]interface{}
	err := json.Unmarshal([]byte(params), &paramsValue)
	if err != nil {
		log.Printf("Edit Plugin json decode params(%s) err :  err: %s", params, err)
	}
	p.Params = paramsValue
}

// 持久化到ES插件
type SaveES struct {
	Params map[string]interface{}
}

func (p SaveES) Handle(m Matedate) Matedate {
	messages <- m
	return m
}


func (p *SaveES) setParams(params string) {
	var paramsValue map[string]interface{}
	err := json.Unmarshal([]byte(params), &paramsValue)
	if err != nil {
		log.Printf("SaveES Plugin json decode params(%s) err :  err: %s", params, err)
	}
	p.Params = paramsValue
}

// 打印
type Dump struct {
	Params map[string]interface{}
}

func (p *Dump) Handle(m Matedate) Matedate {
	for k, v := range m {
		fmt.Printf("%s : %s\n", k, v)
	}
	fmt.Println("------------")
	return m
}


func (p Dump) setParams(params string) {
	var paramsValue map[string]interface{}
	err := json.Unmarshal([]byte(params), &paramsValue)
	if err != nil {
		log.Printf("Dump Plugin json decode params(%s) err :  err: %s", params, err)
	}
	p.Params = paramsValue
}

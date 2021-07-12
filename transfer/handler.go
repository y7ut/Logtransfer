package transfer

import (
	"fmt"
)

type Matedate map[string]interface{}

type Handler func(Matedate) Matedate

type PipeLine struct {
	Next    *PipeLine
	Current Handler
}

func (p PipeLine) Enter(m Matedate) Matedate {
	if p.Current == nil {
		return m
	}
	if p.Next == nil {
		return p.Current(m)
	}
	return p.Next.Enter(p.Current(m))
}

// 持久化到ES
func Edit(m Matedate) Matedate {
	m["source"] = "logtranfers"
	return m
}

// 持久化到ES
func SaveES(m Matedate) Matedate {
	messages <- m
	return m
}

// 打印
func Dump(m Matedate) Matedate {

	for k, v := range m {
		fmt.Printf("%s : %s\n", k, v)
	}
	fmt.Println("------------")
	return m

}

package plugin

import (
	"fmt"
	"log"

	"github.com/y7ut/logtransfer/entity"
)

// 打印插件
type Dump Plugin

func (dump *Dump) HandleFunc(m *entity.Matedata) error {
	log.Println("DUMP:")
	for k, v := range (*m).Data {
		fmt.Printf("%s : %s\n", k, v)
	}
	fmt.Println("------------")
	return nil
}

func (dump *Dump) SetParams(params string) error {
	return nil
}

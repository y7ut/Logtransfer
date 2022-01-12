package plugin

import (
	"fmt"
	"log"
	"time"

	"github.com/y7ut/logtransfer/entity"
)

const (
	Location = "Asia/Shanghai"
)

// 打印插件
type Dump Plugin

func (dump *Dump) HandleFunc(m *entity.Matedata) error {
	log.Println("DUMP:")
	for k, v := range (*m).Data {
		if k == "timestamp" {
			loc, err := time.LoadLocation(Location)
			if err != nil {
				loc = time.FixedZone("CST", 8*3600)
			}
			v, err = time.ParseInLocation("2006-01-02 15:04:05 ", fmt.Sprintf("%s", v), loc)
			if err != nil {
				continue
			}
		}
		fmt.Printf("%s : %s\n", k, v)
	}

	fmt.Println("------------")
	return nil
}

func (dump *Dump) SetParams(params string) error {
	return nil
}

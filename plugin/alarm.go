package plugin

import (
	"fmt"
	"log"
	"time"

	"github.com/y7ut/logtransfer/entity"
)

// 打印插件
type Dump Plugin

func (dump *Dump) HandleFunc(m *entity.Matedata) error {
	log.Println("DUMP:")
	for k, v := range (*m).Data {
		if k == "timestamp" {
			// 这里需要回显 假设现在是UTC-8
			loc := time.FixedZone("UTC", -8*3600)
			createdAt, err := time.ParseInLocation("2006-01-02 15:04:05 ", fmt.Sprintf("%s", v), loc)
			if err != nil {
				continue
			}
			log.Println("DP",createdAt)
			// UTC时间就是+8小时
			v = createdAt.UTC().Format("2006-01-02 15:04:05")
		}
		fmt.Printf("%s : %s\n", k, v)
	}

	fmt.Println("------------")
	return nil
}

func (dump *Dump) SetParams(params string) error {
	return nil
}

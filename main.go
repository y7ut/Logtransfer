package main

import (
	"flag"
	"fmt"

	"github.com/y7ut/logtransfer/transfer"
)

const version = "2.1.0"

var c = flag.String("c", "./logtransfer.conf", "使用配置文件启动")
var v = flag.Bool("v", false, "查看当前程序版本")

func main() {

	// 加载配置文件
	flag.Parse()

	if *v {
		fmt.Println("Jiwei Logtransfer \nversion: ", version)
		return
	}

	transfer.Run(*c)
}

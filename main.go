package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/y7ut/logtransfer/transfer"
)

const version = "2.1.2"

var c = flag.String("c", "./logtransfer.conf", "使用配置文件启动")
var v = flag.Bool("v", false, "查看当前程序版本")
var logPath = flag.String("log", "./log/runtime.log", "日志输出")

func main() {

	// 加载配置文件
	flag.Parse()

	if *v {
		fmt.Println("Jiwei Logtransfer \nversion: ", version)
		return
	}

	writerLog, err := os.OpenFile(*logPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, os.ModeAppend|os.ModePerm)
	if err != nil {
		log.Fatalf("create file log.txt failed: %v", err)
		return
	}
	log.Default().SetFlags(log.LstdFlags)
	log.Default().SetOutput(io.MultiWriter(writerLog, os.Stderr))

	transfer.Run(*c)
}

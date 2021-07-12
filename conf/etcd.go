package conf

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	"gopkg.in/ini.v1"
)

var (
	configPath = "/logagent/config/"
	statusPath = "/logagent/active/"
)

type LogagentConfig []byte

var cli *clientv3.Client

func Init(confPath string) {
	// 加载配置文件
	if err := ini.MapTo(APPConfig, confPath); err != nil {
		log.Println("load ini file error: ", err)
		return
	}
	cli = initConnect()
}

func initConnect() *clientv3.Client {

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{APPConfig.Etcd.Address},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		panic(fmt.Sprintf("connect failed, err:%s \n", err))
	}

	log.Println("connect etcd succ")
	return cli
}

func GetAllConfFromEtcd() []LogagentConfig {

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := cli.Get(ctx, configPath, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))
	cancel()
	if err != nil {
		panic(fmt.Sprintf("get failed, err:%s \n", err))
	}

	configs := make([]LogagentConfig, 0)

	for _, etcdResult := range resp.Kvs {
		
		

		etcdKey := statusPath + string(etcdResult.Key[strings.LastIndex(string(etcdResult.Key), "/")+1:])

		ctx, cancel = context.WithTimeout(context.Background(), time.Second)
		resp, err := cli.Get(ctx, etcdKey)
		cancel()
		if err != nil {
			panic(fmt.Sprintf("Get Etcd config failed, err:%s \n", err))
		}
	
		if len(resp.Kvs) != 0 {
			status := string(resp.Kvs[0].Value)
			if status == "1" {
				log.Printf("load config from:%s ", etcdResult.Key)
				configs = append(configs, etcdResult.Value)
			}
			
		}
		
		
	}

	return configs
}

func WatchLogConfToEtcd() clientv3.WatchChan {

	wch := cli.Watch(context.Background(), configPath, clientv3.WithPrefix())

	return wch
}

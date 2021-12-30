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
	topicPath  = "/logagent/topic/"
)

type EtcdValue []byte

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

	addressList := strings.Split(APPConfig.Etcd.Address, ",")
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   addressList,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		panic(fmt.Sprintf("connect failed, err:%s \n", err))
	}

	log.Println("connect etcd succ")
	return cli
}

// 获取当前开启的Agent所有的任务 (目前在初始化时使用)
func GetAllConfFromEtcd() ([]EtcdValue, error) {

	configs := make([]EtcdValue, 0)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := cli.Get(ctx, configPath, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))
	cancel()
	if err != nil {
		return configs, err
	}

	for _, etcdResult := range resp.Kvs {
		// 根据系统中当前全部的节点名称, 确定节点状态
		etcdKey := statusPath + string(etcdResult.Key[strings.LastIndex(string(etcdResult.Key), "/")+1:])

		ctx, cancel = context.WithTimeout(context.Background(), time.Second)
		resp, err := cli.Get(ctx, etcdKey)
		cancel()
		if err != nil {
			return configs, fmt.Errorf("get Etcd config failed, err:%s", err)
		}

		if len(resp.Kvs) != 0 {
			status := string(resp.Kvs[0].Value)
			if status == "1" {
				log.Printf("load config from:%s ", etcdResult.Key)
				configs = append(configs, etcdResult.Value)
			}

		}

	}

	return configs, nil
}

func GetDelRevValueFromEtcd(key string, rev int64) (EtcdValue, error) {
	var value EtcdValue

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := cli.Get(ctx, key, clientv3.WithRev(rev))
	cancel()
	if err != nil {
		log.Println(fmt.Sprintf("Get Etcd config failed, err:%s \n", err))
	}

	if len(resp.Kvs) == 0 {
		return value, fmt.Errorf("config get error")
	}

	return resp.Kvs[0].Value, nil
}

// 获取特定的配置
func GetConfFromEtcd(name string) (EtcdValue, error) {

	var value EtcdValue

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := cli.Get(ctx, statusPath+name)
	cancel()
	if err != nil {
		log.Println(fmt.Sprintf("Get Etcd config failed, err:%s \n", err))
	}

	if len(resp.Kvs) == 0 {
		return value, fmt.Errorf("status error")
	}
	status := string(resp.Kvs[0].Value)
	if status != "1" {
		return value, fmt.Errorf("status error")
	}

	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	resp, err = cli.Get(ctx, configPath+name)
	cancel()

	if err != nil {
		return value, err
	}

	if len(resp.Kvs) == 0 {
		return value, fmt.Errorf("config get error")
	}

	return resp.Kvs[0].Value, nil
}

// 加载所有的Topic主题配置信息
func GetAllTopicFromEtcd() ([]EtcdValue, error) {

	configs := make([]EtcdValue, 0)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := cli.Get(ctx, topicPath, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))
	cancel()
	if err != nil {
		return configs, err
	}

	for _, etcdResult := range resp.Kvs {
		configs = append(configs, etcdResult.Value)
	}

	return configs, nil
}

// 获取特定的Topic
func GetTopicFromEtcd(name string) (EtcdValue, error) {

	var value EtcdValue

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := cli.Get(ctx, topicPath+name)
	cancel()
	if err != nil {
		log.Println(fmt.Sprintf("Get Etcd config failed, err:%s \n", err))
	}

	if len(resp.Kvs) == 0 {
		return value, fmt.Errorf("config get error")
	}

	return resp.Kvs[0].Value, nil
}

func WatchLogTopicToEtcd() clientv3.WatchChan {

	wch := cli.Watch(context.Background(), topicPath, clientv3.WithPrefix())

	return wch
}

func WatchLogConfigToEtcd() clientv3.WatchChan {

	wch := cli.Watch(context.Background(), configPath, clientv3.WithPrefix())

	return wch
}

func CheckAgentActive(name string) bool {

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := cli.Get(ctx, statusPath+name)
	log.Println("looking for", statusPath+name)
	cancel()
	if err != nil {
		log.Println(fmt.Sprintf("Get Etcd config failed, err:%s \n", err))
	}

	if len(resp.Kvs) == 0 {
		return false
	}
	status := string(resp.Kvs[0].Value)
	log.Println("it is"+status)
	return status == "1"
}

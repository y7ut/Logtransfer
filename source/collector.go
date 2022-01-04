package source

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/y7ut/logtransfer/conf"
	"github.com/y7ut/logtransfer/entity"
	"github.com/y7ut/logtransfer/plugin"
)

type Collector struct {
	Style string `json:"style"`
	Path  string `json:"path"`
	Topic string `json:"topic"`
}

type Topic struct {
	Name     string
	Label    string
	PipeLine *plugin.PipeLine
	Format   entity.Formater
}

type TopicConfig struct {
	Format         int                     `json:"format"`
	Label          string                  `json:"label"`
	Name           string                  `json:"name"`
	PipelineConfig []PipeLinePluginsConfig `json:"piepline"`
}

type PipeLinePluginsConfig struct {
	Label  string `json:"label"`
	Name   string `json:"name"`
	Params string `json:"params"`
}

var watchTopicChannel = make(chan *Topic)

var startTopicChannel = make(chan *Topic)

var deleteTopicChannel = make(chan string)

// 加载所有的可用的collector
func LoadCollectors() ([]Collector, error) {
	collectors := make([]Collector, 0)
	configs, err := conf.GetAllConfFromEtcd()
	if err != nil {
		return collectors, err
	}
	for _, v := range configs {

		var currentCollector []Collector
		err := json.Unmarshal(v, &currentCollector)
		if err != nil {
			log.Printf("json decode config(%s) err :  err: %s", v, err)
		}

		if currentCollector != nil {
			log.Printf("Init config:%s ", v)
			collectors = append(collectors, currentCollector...)
		}
	}
	return collectors, nil
}

// 加载Agent所有的collector
func LoadCollector(name string) ([]Collector, error) {
	var collectors []Collector
	config, err := conf.GetConfFromEtcd(name)
	if err != nil {
		return collectors, fmt.Errorf("get etcd config err :  err: %s", err)
	}

	err = json.Unmarshal(config, &collectors)
	if err != nil {
		return collectors, fmt.Errorf("json decode config(%s) err :  err: %s", collectors, err)
	}

	return collectors, nil
}

// 收集所有需要监听的topic
func ChooseTopic() (map[*Topic]bool, error) {
	// 收集全部的agent的collector信息
	ableTopics := make(map[*Topic]bool)

	// 所有当前
	collectors, err := LoadCollectors()
	if err != nil {
		return ableTopics, fmt.Errorf("Load Collector error: %s", err)
	}

	topics, err := loadTopics()
	if err != nil {
		return ableTopics, fmt.Errorf("Load Topic error: %s", err)
	}

	for _, v := range collectors {
		currentTopic := topics[v.Topic]
		ableTopics[currentTopic] = true
	}

	return ableTopics, nil
}

// 解析全部的Topic并加载内部的格式器和插件pipeline
func loadTopics() (map[string]*Topic, error) {

	topics := make(map[string]*Topic)

	configs, err := conf.GetAllTopicFromEtcd()

	if err != nil {
		return topics, err
	}

	for _, v := range configs {

		var currentTopicConfig TopicConfig
		err := json.Unmarshal(v, &currentTopicConfig)
		if err != nil {
			return topics, err
		}

		topics[currentTopicConfig.Name] = generateTopic(currentTopicConfig)
	}
	return topics, nil
}

func TopicChangeListener() <-chan *Topic {
	return watchTopicChannel
}

func TopicDeleteListener() <-chan string {
	return deleteTopicChannel
}

func TopicStartListener() <-chan *Topic {
	return startTopicChannel
}

func WatchTopics() {
	for confResp := range conf.WatchLogTopicToEtcd() {

		for _, event := range confResp.Events {
			switch event.Type {
			case mvccpb.PUT:
				// 有PUT操作才进行通知
				var newTopicCondfig TopicConfig
				if len(confResp.Events) == 0 {
					continue
				}
				changedConf := confResp.Events[0].Kv.Value

				err := json.Unmarshal(changedConf, &newTopicCondfig)

				if err != nil {
					log.Println("Unmarshal New Topic Config Error:", err)
				}
				log.Println("load New Topic success!")

				watchTopicChannel <- generateTopic(newTopicCondfig)

			case mvccpb.DELETE:
				// 获取旧版本数据 来进行对比
				// 要清空这个register 中全部这个topic的customer， 不过也应该没有了，应该都会被config watcher 给捕获到
				oldTopic, err := getHistoryTopicWithEvent(confResp)
				if err != nil {
					log.Println("Get HIstory Collector Error:", err)
					continue
				}

				log.Println("some Topic remove", oldTopic.Name)
			}

			time.Sleep(2 * time.Second)
		}

	}
}

func WatchStatus() {
	for confResp := range conf.WatchLogStatusToEtcd() {
		// 如果是关闭的Agent 这些可以忽略的！！！
		agentKey := string(confResp.Events[0].Kv.Key)
		currentChangedkey := agentKey[strings.LastIndex(agentKey, "/")+1:]

		// 只有开启的才去更改配置
		for _, event := range confResp.Events {
			switch event.Type {
			case mvccpb.PUT:
				if len(confResp.Events) == 0 {
					continue
				}
				// 如果是1 name就是从0开启的（开启）
				// 如果是0有可能是从1关闭的(关闭)，也有可能是突然新增的(无视)
				collectors, status, err := getStatusChangeWithEvent(confResp)
				if err != nil {
					log.Println("Get Status Active Event Info Error:", err)
					continue
				}
				log.Println("STATUS CHANGE", status)
				switch status {
				case "CREATED":
					// 情况三 可以不进行操作
					// 不会出现，因为初始化的时候一定是关闭的。
					log.Println("Add Agent Empty Status :", currentChangedkey)
				case "CLOSE":
					for _, c := range collectors {
						log.Println("有collector离开")
						// 获取config prefix 全部collector，判断是否是最后一个用这个topic的collector
						currentAbleCollector, err := LoadCollectors()

						if err != nil {
							log.Println("Get Current Able Collector Error:", err)
							continue
						}
						var set = make(map[string]bool)
						for _, v := range currentAbleCollector {
							set[v.Topic] = true
						}

						if !set[c.Topic] {
							// 可以删了
							deleteTopicChannel <- c.Topic
							log.Println(currentChangedkey, "Agent Close Delete Collector with Topic Customer left", c.Topic)
							continue
						}

						log.Println(currentChangedkey, "Agent Close with Close Collector", c)
					}
				case "OPEN":
					for _, c := range collectors {
						_, ok := GetCustomer(c.Topic)
						// 如果首次出现 那就初始化这个Topic了
						// 根据topicname去获取TopicConfig
						if !ok {
							changedConf, err := conf.GetTopicFromEtcd(c.Topic)
							if err != nil {
								log.Println("Load Agent Open New Puted Topic Config Error:", err)
								continue
							}
							// 有PUT操作才进行通知
							var newPutTopicCondfig TopicConfig

							err = json.Unmarshal(changedConf, &newPutTopicCondfig)

							if err != nil {
								log.Println("Unmarshal Agent Open New Puted Topic Config Error:", err)
								continue
							}
							log.Println("load Agent New Puted Topic success!")

							startTopicChannel <- generateTopic(newPutTopicCondfig)
							log.Println(currentChangedkey, "Agent open with open Collector And Init Topic Customer", c)
						}
						log.Println(currentChangedkey, "Agent open with open Collector, But has opend", c)
					}
				default:

					log.Println("Get Status Active Event Info Unkonw Error")
				}

			}
		}
	}
}

func WatchConfigs() {
	for confResp := range conf.WatchLogConfigToEtcd() {
		// 如果是关闭的Agent 这些可以忽略的！！！
		agentKey := string(confResp.Events[0].Kv.Key)
		currentChangedkey := agentKey[strings.LastIndex(agentKey, "/")+1:]
		if !conf.CheckAgentActive(currentChangedkey) {
			continue
		}
		// 只有开启的才去更改配置
		for _, event := range confResp.Events {
			switch event.Type {
			case mvccpb.PUT:
				if len(confResp.Events) == 0 {
					continue
				}
				// 有PUT操作才进行通知
				// agent的PUT有多种情况
				// 一: 通过REV查询 发现是Agent新增了一个Collector （len > 0）
				// 然后再去查询内存中 的registed customer 就可以了 判断这个Collector所使用的topic是否是第一次出现（安装对应的Topic）
				// 二: 通过REV查询 发现出删除了了一个Collector （len >= 0）
				// 然后要再去获取config prefix 全部collector，判断是否是最后一个用这个topic的collector（需要卸载这个collector所使用的Topic）
				// 三: 通过REV查询 发现之前不存在的 （len = 0）
				diff, status, err := getCollectorChangeWithEvent(confResp)

				if err != nil {
					log.Println("Get History Collector Change Error:", err)
					continue
				}
				switch status {
				case "CREATED":
					// 情况三 可以不进行操作
					// 不会出现，因为初始化的时候一定是关闭的。
					log.Println("Add Agent", currentChangedkey)
				case "PUT":
					log.Println("有collector加入")
					_, ok := GetCustomer(diff.Topic)
					// 如果首次出现 那就初始化这个Topic了
					// 根据topicname去获取TopicConfig
					if !ok {
						changedConf, err := conf.GetTopicFromEtcd(diff.Topic)
						if err != nil {
							log.Println("Load Agent New Puted Topic Config Error:", err)
							continue
						}
						// 有PUT操作才进行通知
						var newPutTopicCondfig TopicConfig

						err = json.Unmarshal(changedConf, &newPutTopicCondfig)

						if err != nil {
							log.Println("Unmarshal Agent New Puted Topic Config Error:", err)
							continue
						}
						log.Println("load Agent New Puted Topic success!")

						startTopicChannel <- generateTopic(newPutTopicCondfig)
						log.Println(currentChangedkey, "Agent Add Collector And Init Topic Customer", diff)
					}
					log.Println(currentChangedkey, "Agent Add Collector", diff)
				case "DEL":
					log.Println("有collector离开")
					// 获取config prefix 全部collector，判断是否是最后一个用这个topic的collector
					currentAbleCollector, err := LoadCollectors()

					if err != nil {
						log.Println("Get Current Able Collector Error:", err)
						continue
					}
					var set = make(map[string]bool)
					for _, v := range currentAbleCollector {
						set[v.Topic] = true
					}

					if !set[diff.Topic] {
						// 可以删了
						deleteTopicChannel <- diff.Topic
						log.Println(currentChangedkey, "Agent Delete Collector with Topic Customer left", diff)
						continue
					}

					log.Println(currentChangedkey, "Agent Delete Collector", diff)
				default:
					log.Println("Get History Collector Change Unkonw Error!")
				}

			case mvccpb.DELETE:
				// 获取旧版本数据 来进行对比
				// 通常这个地方应该知识空数组了
				// 因为agent不允许在有collector的时候删除
				// 也不允许 在开启的时候删除~
				// 所以这个地方没有用
				if len(confResp.Events) == 0 {
					continue
				}

				oldCollector, err := getHistoryCollectorWithEvent(confResp)
				if err != nil {
					log.Println("Get History Collector Error:", err)
					continue
				}
				if len(oldCollector) != 0 {
					log.Printf("Get History Collector Error: Agent(%s) die with collector.", confResp.Events[0].Kv.Key)
					continue
				}

				log.Printf("Agent(%s) has uninstall complete.", confResp.Events[0].Kv.Key)
			}
		}

	}
}

func getHistoryTopicWithEvent(confResp clientv3.WatchResponse) (TopicConfig, error) {

	var oldTopic TopicConfig

	oldKey := confResp.Events[0].Kv.Key
	rev := confResp.Events[0].Kv.ModRevision - 1

	oldValue, err := conf.GetDelRevValueFromEtcd(string(oldKey), rev)

	if err != nil {
		return oldTopic, err
	}

	err = json.Unmarshal(oldValue, &oldTopic)

	if err != nil {
		return oldTopic, err
	}

	return oldTopic, nil
}

func getHistoryCollectorWithEvent(confResp clientv3.WatchResponse) ([]Collector, error) {

	var oldCollector []Collector

	oldKey := confResp.Events[0].Kv.Key
	rev := confResp.Events[0].Kv.ModRevision - 1

	oldValue, err := conf.GetDelRevValueFromEtcd(string(oldKey), rev)

	if err != nil {
		return oldCollector, err
	}

	err = json.Unmarshal(oldValue, &oldCollector)

	if err != nil {
		return oldCollector, err
	}

	return oldCollector, nil
}

// 获取Agent 中 Collector 的变更 有三种类型 CREATED  新增Agent PUT 新增Collector DEL 删除 Collector
func getCollectorChangeWithEvent(confResp clientv3.WatchResponse) (different Collector, changeType string, err error) {

	var currentCollectors []Collector

	changedConf := confResp.Events[0].Kv.Value

	err = json.Unmarshal(changedConf, &currentCollectors)
	if err != nil {
		return different, changeType, err
	}

	var oldCollector []Collector

	oldKey := confResp.Events[0].Kv.Key
	rev := confResp.Events[0].Kv.ModRevision - 1

	oldValue, err := conf.GetDelRevValueFromEtcd(string(oldKey), rev)

	if err != nil {
		if len(currentCollectors) == 0 {
			changeType = "CREATED"
			err = nil
		}
		return different, changeType, err
	}

	err = json.Unmarshal(oldValue, &oldCollector)

	if err != nil {
		return different, changeType, err
	}

	var set = make(map[Collector]bool)

	if len(oldCollector)-len(currentCollectors) < 0 {
		changeType = "PUT"

		for _, item := range oldCollector {
			set[item] = true
		}

		for _, item := range currentCollectors {
			if !set[item] {
				different = item
			}
		}
	} else {
		changeType = "DEL"
		for _, item := range currentCollectors {
			set[item] = true
		}

		for _, item := range oldCollector {
			if !set[item] {
				different = item
			}
		}
	}

	return different, changeType, err
}

func getStatusChangeWithEvent(confResp clientv3.WatchResponse) (collectors []Collector, changeType string, err error) {

	changeStatus := fmt.Sprintf("%s", confResp.Events[0].Kv.Value)

	// 先对比一下
	oldKey := confResp.Events[0].Kv.Key
	rev := confResp.Events[0].Kv.ModRevision - 1

	oldValue, err := conf.GetDelRevValueFromEtcd(string(oldKey), rev)

	if err != nil {
		if len(oldValue) == 0 {
			// 新来的可以无视
			changeType = "CREATE"
			err = nil
			return collectors, changeType, err
		}
		return collectors, changeType, err
	}

	// 获取这个agent的内容
	agentKey := string(confResp.Events[0].Kv.Key)
	currentChangedkey := agentKey[strings.LastIndex(agentKey, "/")+1:]

	AllCollectors, err := LoadCollector(currentChangedkey)

	if err != nil {
		return collectors, changeType, err
	}

	changeType = "CLOSE"

	if changeStatus == "1" {
		changeType = "OPEN"
	}

	var topicSet = make(map[string]bool)

	for _, c := range AllCollectors {
		if topicSet[c.Topic] {
			continue
		}
		topicSet[c.Topic] = true
		collectors = append(collectors, c)
	}

	return collectors, changeType, err
}

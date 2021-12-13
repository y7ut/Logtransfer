package conf

type LogTransferConf struct {
	Kafka `ini:"kafka"`
	Etcd  `ini:"etcd"`
	Es    `ini:"es"`
}

// kafka 配置
type Kafka struct {
	Address   string `ini:"address"`
}

// ETCD 配置
type Etcd struct {
	Address string `ini:"address"`
}

// Es 属性
type Es struct {
	Address string `ini:"address"`
	BulkSize int `ini:"bulk_size"`
}

var (
	APPConfig = new(LogTransferConf)
)
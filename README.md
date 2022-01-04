### LogTransfer Customer

# 原理
从Etcd中获取所有在注册的配置，收集需要消费的Topic和对应信息。

# 版本
### 1.0.0
1. 从kafka消费数据
2. 管道的形式处理数据
3. 提供数据同步到ES的插件

### 2.0.0
1. 支持动态的安装卸载插件

### 2.1.0
1. 添加动态的配置监控
2. 添加ES消息存储的协程池

### 2.1.1
1. 优化退出信号的监听
2. 修改ES Save的Deadline

# 安装
```
docker build -t logtransfer:version .  
docker run -d --name=LTF logtransfer:version
```

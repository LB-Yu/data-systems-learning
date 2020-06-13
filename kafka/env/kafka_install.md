本文讲述Kafka的不同安装模式, 详细配置和简单测试.

安装前现在[Kafka Download](http://kafka.apache.org/downloads)页面下载Kafka安装包, 并将下载的安装包复制到安装目录, 本文为`/usr/local`. 本文以`kafka_2.12-2.5.0.tgz`为例进行安装, 各不同版本之间的安装方式基本相同. 

## 单机模式
单机模式仅在一台机器上启动一个broker. 常在开发环境中用于测试. 安装方法如下:
```shell
cd /usr/local
sudo tar -zxvf kafka_2.12-2.5.0
cd ..
# 赋予权限, 否则在启动时可能出现Permission deny错误
sudo chmod 777 kafka_2.12-2.5.0
```

更改`/usr/local/kafka_2.12-2.5.0/config/server.properties`中的`log.dirs`属性, Kafka broker的相关配置均在此文件中, 在单机模式下只需修改`log.dirs`这一属性即可, 表示Kafka日志的存储位置.
```shell
log.dirs=/usr/local/kafka_2.12-2.5.0/data
```

启动Zookeeper服务.
```shell
zkServer.sh start
```

启动Kafka.
```shell
./bin/kafka-server-start.sh -daemon ./config/server.properties
```

启动成功后通过`jps`命令可看到如下虚拟机进程.
```shell
11971 Kafka
```

下面通过Kafka提供的脚本命令进行简单地创建主题, 使用生产者向主题发送消息, 消费者接受消息等测试.

+ 创建一个副本数为1, 分区数为1的`test`主题.
```shell
./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
```
+ 查看主题情况.
```shell
./bin/kafka-topics.sh --zookeeper localhost:2181 --describe --topic test

# 运行上述命令后, 将会得到如下输出
Topic: test	PartitionCount: 1	ReplicationFactor: 1	Configs: 
	Topic: test	Partition: 0	Leader: 0	Replicas: 0	Isr: 0
```
+ 生产者向`test`主题发布消息.
```shell
./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test
>Test Message 1
>Test Message 2
>^D
```
+ 消费者从`test`主题读取消息.
```shell
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning
Test Message 1
Test Message 2
^CProcessed a total of 2 messages
```

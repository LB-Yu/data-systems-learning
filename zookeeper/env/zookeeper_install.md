Apache Zookeeper有三种安装模式: [单机模式(standalone mode)](#%e5%8d%95%e6%9c%ba%e6%a8%a1%e5%bc%8fstandalone-mode), [伪分布式模式(pseudo-distributed mode)](#%e4%bc%aa%e5%88%86%e5%b8%83%e5%bc%8f%e6%a8%a1%e5%bc%8fpseudo-distributed-mode)和[完全分布式模式(Fully distributed mode)](#%e5%ae%8c%e5%85%a8%e5%88%86%e5%b8%83%e5%bc%8f%e6%a8%a1%e5%bc%8ffully-distributed-mode).

本文将详细讲述Zookeeper各种模式的安装方法(基于Linux环境), 并简要阐述各安装模式的应用场景和基本原理.

安装前在[Zookeeper release](http://zookeeper.apache.org/releases.html)页面下载最新版本的Zookeeper, 并将下载的安装包复制到安装目录, 本文为`/usr/local`. 本文使用Zookeeper 3.6.0进行安装, 安装方法也适用于其他版本. 

## 单机模式(Standalone Mode)
单机模式仅在一台机器上启动一个Zookeeper进程. 常在开发环境中用于测试. 安装方法如下:
```shell
cd /usr/local
sudo tar -zxvf apache-zookeeper-3.6.0-bin.tar.gz
sudo mv apache-zookeeper-3.6.0-bin zookeeper-3.6.0
cd ..
# 赋予权限, 否则在启动时可能出现Permission deny错误
sudo chmod 777 zookeeper-3.6.0
```
复制`/usr/local/zookeeper-3.6.0/conf`下的`zoo_sample.cfg`文件, 并更名为`zoo.cfg`， 在`zoo.cfg`中写入如下内容:
```shell
tickTime=2000
initLimit=10
syncLimit=5
dataDir=/usr/local/zookeeper-3.6.0/data/zoo
clientPort=2181
server.1=localhost:2888:3888
```
启动Zookeeper:
```shell
cd zookeeper-3.6.0
./bin/zkServer.sh start
```
启动成功将看到如下内容:
```shell
ZooKeeper JMX enabled by default
Using config: /usr/local/zookeeper-3.6.0/bin/../conf/zoo.cfg
Starting zookeeper ... STARTED
```
验证启动是否成功. 在shell中输入`jps`命令若看到如下进程则表示启动成功, 其中14877可能为不同的数.
```shell
14877 QuorumPeerMain
```
输入`./bin/zkServer.sh status`命令可查看Zookeeper信息， 结果如下, 可以看到当前启动模式为standalone.
```shell
ZooKeeper JMX enabled by default
Using config: /usr/local/zookeeper-3.6.0/bin/../conf/zoo.cfg
Client port found: 2181. Client address: localhost.
Mode: standalone
```

## 伪分布式模式(Pseudo-distributed Mode)
伪分布式模式在单机环境下启动多个Zookeeper进程, 以模拟集群环境. 伪分布式模式是配置最为复杂的模式, 主要是由于在单机环境下, 多个进程的端口必须予以区分.

解压方法同单机模式, 此处不再赘述. 在`/usr/local/zookeeper-3.6.0/conf`目录下新建三个文件`zoo1.cfg`, `zoo2.cfg`, `zoo3.cfg`并分别写入如下配置.
```shell
# zoo1.cfg
tickTime=2000
initLimit=10
syncLimit=5
dataDir=/usr/local/zookeeper-3.6.0/data/zoo1
clientPort=2181
server.1=localhost:2888:3888
server.2=localhost:2889:3889
server.3=localhost:2890:3890

# zoo2.cfg
tickTime=2000
initLimit=10
syncLimit=5
# 注意需与zoo1.cfg不同
dataDir=/usr/local/zookeeper-3.6.0/data/zoo2
# 注意需与zoo1.cfg不同
clientPort=2182
server.1=localhost:2888:3888
server.2=localhost:2889:3889
server.3=localhost:2890:3890

# zoo2.cfg
tickTime=2000
initLimit=10
syncLimit=5
# 注意需与zoo1.cfg, zoo2.cfg不同
dataDir=/usr/local/zookeeper-3.6.0/data/zoo3
# 注意需与zoo1.cfg, zoo2.cfg不同
clientPort=2183
server.1=localhost:2888:3888
server.2=localhost:2889:3889
server.3=localhost:2890:3890
```
在`zoo1.cfg`, `zoo2.cfg`, `zoo3.cfg`文件中指定的`dataDir`路径下创建`myid`文件, 并分别写入`1`, `2`, `3`.

启动Zookeeper.
```shell
./bin zkServer.sh start zoo1.cfg
./bin zkServer.sh start zoo2.cfg
./bin zkServer.sh start zoo3.cfg
```

## 完全分布式模式(Fully Distributed Mode)
完全分布式模式用于生产环境. 需要在每台机器的相同目录下安装Zookeeper. 解压方法同单机模式, 此处不再赘述. 

复制`/usr/local/zookeeper-3.6.0/conf`下的`zoo_sample.cfg`文件, 并更名为`zoo.cfg`， 在`zoo.cfg`中写入如下内容:
```shell
tickTime=2000
initLimit=10
syncLimit=5
dataDir=/usr/local/zookeeper-3.6.0/data
clientPort=2181
server.1=localhost:2888:3888
server.2=localhost:2888:3888
server.3=localhost:2888:3888
```
在`zoo.cfg`文件中指定的`dataDir`路径下创建`myid`文件, 并分别写入对应的数字.

启动Zookeeper, 在每台机器执行如下命令.
```shell
./bin zkServer.sh start
```
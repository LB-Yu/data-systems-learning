本文讲述Hadoop的单机伪分布式安装方式. 首先在[下载页面](https://archive.apache.org/dist/hadoop/common/)下载所需版本(如果仅使用Hadoop可选择最新的稳定版, 若需要与其他组件一起使用注意适配相应版本), 并将安装包复制到安装目录, 本文以`/usr/local`为例, 使用Hadoop 2.7.7进行安装.

在安装前配置如下环境变量, 后续操作将基于下述环境变量.
```shell
export HADOOP_HOME=/usr/local/hadoop-2.7.7
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
```

1. 首先解压安装包.
```shell
sudo tar -zxvf hadoop-2.7.7.tar.gz
sudo chmod -R 777 hadoop-2.7.7
```

2. 在`$HADOOP_HOME/etc/hadoop/hadoop-env.sh`中加入如下内容.
```shell
export JAVA_HOME=/usr/local/jdk1.8.0_261
```

3. 在`$HADOOP_HOME/etc/hadoop/core-site.xml`中加入如下内容.
```xml
<configuration>

    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
    
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/usr/local/hadoop-2.7.7/tmp</value>
    </property>

</configuration>
```

4. 在`$HADOOP_HOME/etc/hadoop/hdfs-site.xml`中加入如下内容.
```xml
<configuration>

    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>

</configuration>
```
完成上述配置后即可启动HDFS, 在第一次启动时需要格式化.
```shell
# 第一次启动, 格式化
hdfs namenode -format
# 启动HDFS
start-dfs.sh
# 关闭HDFS
stop-dfs.sh
```

5. 在`$HADOOP_HOME/etc/hadoop/mapred-site.xml`中添加如下内容.
```xml
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
</configuration>
```

6. 在`$HADOOP_HOME/etc/hadoop/yarn-site.xml`中添加如下内容.
```xml
<configuration>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
</configuration>
```
完成步骤5, 6后即可启动yarn.
```shell
# 启动yarn
start-yarn.sh
# 关闭yarn
stop-yarn.sh
```
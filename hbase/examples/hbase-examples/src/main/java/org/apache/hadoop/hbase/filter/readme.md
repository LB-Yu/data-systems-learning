## HBase自定义过滤器

当前包中的代码演示了如何开发HBase自定义过滤器. 运行步骤如下:

```shell
git clone https://github.com/LB-Yu/bigdata_basic.git
cd ./bigdata_basic/hbase/exanples/hbase-examples/
mvn clean package -DskipTests
# ${HBASE_HOME}为HBase安装目录, 若为集群安装, 需要将jar包复制到每个RegionServer的安装目录下
cp ./target/hbase-examples-1.4.9.jar ${HBASE_HOME}/lib/
# 重启HBase
stop-hbase.sh
start-hbase.sh
# 创建测试表
cd src/main/shell/filter/filter_test.txt
hbase shell filter_test.txt
```

运行CustomFilterExample即可.
# data-writer组件

写HDFS文件、Kafka表、Redis。

## 思路

- 期望使用kafka-connect-couchbbase代替KafkaInterface项目modify-new分支Kafka2CouchBase。
  no,最新的kafka-connect-couchbbase支持的kafka版本为0.10.1.1，并且默认存储到couchbase中的key默认为kafka中的key（可以配置更改）,
  具体到ttl的设置，显得没有代码灵活。
  并且目前只测试单机的distribue模式通过，本身测试需要时间，并且不灵活。弃用这种方式。
  
- 统一使用轻量快速的jackson包操作json数据结构
com.fasterxml.jackson.databind.ObjectMapper
kafka connection本身也使用这个包

## 数据结构

## 数据接口
### 写入kafka
  DataFrame to Kafka
  
### 读kafka,写入其他组件
- Kafka to Couchbase
- Kafka to Redis

## kafka的topic命名规则
生产环境
couchbase-moretv
couchbase-medusa
redis-moretv

/opt/kafka5/bin/kafka-topics.sh --create --zookeeper bigdata-appsvr-130-4:2185 --replication-factor 3 --partitions 9 --topic couchbase-moretv
/opt/kafka5/bin/kafka-topics.sh --create --zookeeper bigdata-appsvr-130-4:2185 --replication-factor 3 --partitions 9 --topic couchbase-medusa
/opt/kafka5/bin/kafka-topics.sh --create --zookeeper bigdata-appsvr-130-4:2185 --replication-factor 3 --partitions 9 --topic couchbase-helios
/opt/kafka5/bin/kafka-topics.sh --create --zookeeper bigdata-appsvr-130-4:2185 --replication-factor 3 --partitions 9 --topic redis-moretv
/opt/kafka5/bin/kafka-topics.sh --create --zookeeper bigdata-appsvr-130-4:2185 --replication-factor 3 --partitions 9 --topic redis-helios


测试环境
couchbase-moretv-topic-test
couchbase-medusa-topic-test
couchbase-helios-topic-test
redis-moretv-topic-test
redis-helios-topic-test

/opt/kafka5/bin/kafka-topics.sh --create --zookeeper bigdata-appsvr-130-4:2185 --replication-factor 3 --partitions 9 --topic couchbase-medusa-topic-test

## kafka消费组定义
couchbase-moretv-consume-group
couchbase-helios-consume-group
redis-helios-consume-group
redis-helios-consume-group

### 创建topic
/opt/kafka5/bin/kafka-topics.sh --create --zookeeper bigdata-appsvr-130-4:2185 --replication-factor 3 --partitions 9 --topic redis-moretv-topic-test

### 查看topic中的流入数据
/opt/kafka5/bin/kafka-console-consumer.sh --bootstrap-server bigdata-appsvr-130-4:9095 --topic redis-moretv-topic-test
/opt/kafka5/bin/kafka-console-consumer.sh --bootstrap-server bigdata-appsvr-130-4:9095 --topic couchbase-moretv-topic-test|head


nohup sh /opt/kafka5/bin/kafka-console-consumer.sh --bootstrap-server bigdata-appsvr-130-4:9095 --topic redis-moretv | head    > 1.log 2>&1 &
nohup sh /opt/kafka5/bin/kafka-console-consumer.sh --bootstrap-server bigdata-appsvr-130-4:9095 --topic couchbase-moretv | head  > 2.log 2>&1 &

### 查看topic中 
 /opt/kafka5/bin/kafka-consumer-groups.sh --describe --zookeeper bigdata-appsvr-130-4:2185 --group redis-moretv-consume-group-test --topic redis-moretv-topic-test
 /opt/kafka5/bin/kafka-consumer-groups.sh --describe --zookeeper bigdata-appsvr-130-4:2185 --group couchbase-moretv-consume-group-test
 sh /opt/kafka5/bin/kafka-console-consumer.sh --bootstrap-server bigdata-appsvr-130-4:9095 --topic redis-moretv  --from-beginning |grep ZSET|head


## 查看topic消费组的lag
1.查看生产环境couchbase kafka的延迟
/opt/kafka5/bin/kafka-consumer-groups.sh --bootstrap-server bigdata-appsvr-130-4:9095  --describe --group couchbase-moretv-consume-group

/opt/kafka5/bin/kafka-consumer-groups.sh --bootstrap-server bigdata-appsvr-130-4:9095  --describe --group redis-moretv-consume-group-test
/opt/kafka5/bin/kafka-consumer-groups.sh --bootstrap-server bigdata-appsvr-130-4:9095  --describe --group couchbase-moretv-consume-group-test

nohup sh /opt/kafka5/bin/kafka-console-consumer.sh --bootstrap-server bigdata-appsvr-130-4:9095 --topic redis-moretv-topic-test > 2.log 2>&1 &
nohup sh /opt/kafka5/bin/kafka-console-consumer.sh --bootstrap-server bigdata-appsvr-130-4:9095 --topic couchbase-moretv-topic-test > 3.log 2>&1 &

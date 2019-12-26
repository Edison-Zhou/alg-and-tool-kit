#  kafka-sink

kafka中数据写入其他存储系统
kafka-client与couchbase-client本身都是java实现。
couchbase upsert 默认persistTo == PersistTo.NONE && replicateTo == ReplicateTo.NONE,无需单独设置

## 打包方式

 配置文件不会打入jar包，并且不会以classpath方式引入。会通过相对路径读取properties文件     

##  环境配置参数
kafka-sink.properties        生产环境
kafka-sink-dev.properties    开发环境
kafka-sink-test.properties   测试环境

## couchbase数据格式，直接插入value字段中的值

- couchbase数据格式
kafka key randomID
kafka value:{
 "key":"插入couchbase中的key"
 "value":"插入couchbase中的value"
 "ttl":10000 [int,可选,单位为秒，默认为0,表示永久]
}


## redis数据格式,分两类 

- KV
kafka key randomID
kafka value:{
 "key":"插入redis中的key"
 "value":"插入redis中的value"  
 'host':'1.1.1.1'
 'port':8888
 'formatType':'KV'
 'ttl':[int,可选,默认ttl设置为-1，表示永不过期]
}

- Zset格式的kafka信息，遍历value字段中的值
kafka key randomID
kafka value:{
 "key":"插入redis中的key"
 "value":"map结构的json"  
 'host':'1.1.1.1'
 'port':8888
 'formatType':'KV'
  'ttl':[int,可选,默认ttl设置为-1，表示永不过期]
}

## 现网部署

部署机器
bigdata-appsvr-130-8

bigdata-appsvr-130-9

部署位置


## kafka消费组定义
couchbase-moretv-consume-group-test
couchbase-helios-consume-group-test
redis-helios-consume-group-test
redis-helios-consume-group-test


参考：
- zset操作
refinement
cn.whaley.ai.etl.preprocess.tagpreprocess.Utils

- kafkaInterface

- kafka-connect-couchbase开源项目
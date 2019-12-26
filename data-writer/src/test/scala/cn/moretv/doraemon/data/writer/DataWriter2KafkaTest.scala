package cn.moretv.doraemon.data.writer

import cn.moretv.doraemon.common.enum.FormatTypeEnum
import cn.moretv.doraemon.common.path.{RedisPath, CouchbasePath, Path}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructField
import org.junit.{Test, Before}

/**
  * Created by baozhiwang on 2018/6/11.
  */
class DataWriter2KafkaTest {
  val ss = SparkSession
    .builder
    .appName("DataWriter2KafkaTest")
    .master("local")
    .getOrCreate()

  val sc = ss.sparkContext
  val sqlContext = ss.sqlContext
  var dataSource: DataFrame = null

  @Before
  def initData(): Unit = {
    //init in every junit test for different data format
  }



  /**
    *测试DataFrame写入kafka，couchbase相关的数据
    * */
  @Test
  def dataFrame2KafkaForCouchbase(): Unit = {
    import sqlContext.implicits._
    val param = new DataPackParam
    param.format = FormatTypeEnum.KV
    param.extraValueMap=Map("alg"->"dl_v2","timestamp"->20180629071925l)
    param.keyPrefix="c:m:"


    val path1:Path= CouchbasePath("couchbase-moretv-topic-test")
    val dataWriter:DataWriter=new DataWriter2Kafka

    //测试数据集
    val dataSample1=Seq(("uid1", "[11,12]"),("uid2", "[21,22]"))
    //id 为c:m:1000000034282992820中的id
    dataSource = DataPack.pack(dataSample1.toDF("uid", "id"), param)
    dataWriter.write(dataSource,path1)

    val ttl2=172800
    val path2:Path= CouchbasePath("couchbase-moretv-topic-test",ttl2)
    val dataSample2=Seq(("uid3",Array(31,32)),("uid4",Array(41,42)))
    dataSource = DataPack.pack(dataSample2.toDF("uid", "id"), param)
    dataWriter.write(dataSource,path2)

  }

  @Test
 def array2DataFrame: Unit ={
    import sqlContext.implicits._
    val dataSample2=Seq(("key3",Array(31,32)),("key4",Array(41,42)))
    dataSample2.toDF("uid", "id").show()
  }

  /**
    *测试DataFrame写入kafka，redis相关的数据,KV类型
    * */
  @Test
  def dataFrame2KafkaForRedisKV(): Unit = {
    //mock data
    val topic="redis-moretv-topic-test"
    val host="bigtest-cmpt-129-204"
    val port=6379
    val dbIndex=0
    val ttl=8640000
    val formatType=FormatTypeEnum.KV

    import sqlContext.implicits._
    dataSource = Seq(
      (21,   21),
      (22,   22),
      (23,   23)
    ).toDF("key","value")
    dataSource.schema.printTreeString()


    val path:Path= RedisPath(topic,host,port,dbIndex,ttl,formatType)
    val dataWriter:DataWriter=new DataWriter2Kafka
    dataWriter.write(dataSource,path)
  }

  /**
    *测试DataFrame写入kafka，redis相关的数据,Zset类型
    * */
  @Test
  def dataFrame2KafkaForRedisZset(): Unit = {
    import sqlContext.implicits._

    val df3 = Seq(
      ("a", Array(("aa", 0.3), ("aaa", 0.2))),
      ("b", Array(("bb", 0.4), ("bbb", 0.2), ("bbbb", 0.3)))
    ).toDF("u", "s")

    val param = new DataPackParam
    param.format = FormatTypeEnum.ZSET
    dataSource = DataPack.pack(df3, param)

    //mock data
    val topic="redis-moretv-topic-test"
    val host="bigtest-cmpt-129-204"
    val port=6379
    val dbIndex=1
    val ttl=8640000
    val formatType=FormatTypeEnum.ZSET

    val path:Path= RedisPath(topic,host,port,dbIndex,ttl,formatType)
    val dataWriter:DataWriter=new DataWriter2Kafka
    dataWriter.write(dataSource,path)
  }

  @Test
  def writeOneTest(): Unit = {
    val dataWriter:DataWriter2Kafka=new DataWriter2Kafka
    val topic="redis-moretv-topic-dev"
    val host="bigtest-cmpt-129-204"
    val port=6379
    val dbIndex=4
    val ttl=8640000
    val formatType=FormatTypeEnum.HASH

    val path:Path= RedisPath(topic,host,port,dbIndex,ttl,formatType)
    val map = Map("key"->"a", "value" -> Map("alg"->"alg", "biz"->"biz"))
    dataWriter.writeOne(map, path)
    dataWriter.closeInstance()
  }

  @Test
  def writeOneToCouchbaseTest(): Unit = {
    val dataWriter:DataWriter2Kafka=new DataWriter2Kafka
    val topic="couchbase-medusa-topic-dev"
    val ttl=8640000
    val value= "{\"key\":\"p:a:88888888\",\"value\":\"{\\\"portal\\\":{\\\"id\\\":[\\\"fh8rtvuvce7o\\\",\\\"e5suqra2ceij\\\",\\\"tvn82cn82d6k\\\",\\\"tvwywxmof5ce\\\",\\\"tvwy6ktvd3n8\\\",\\\"g6abtvcdv0t9\\\",\\\"tvn86lab6kab\\\",\\\"e5uvpq7pe534\\\",\\\"tvn82c5in8c3\\\",\\\"4g1c9wikfhu9\\\",\\\"g6hi45ikhjd4\\\",\\\"e56k5hprhjij\\\",\\\"fhoqxyvw5i9w\\\",\\\"tvwybcw0bch6\\\",\\\"fh3fhin8234g\\\",\\\"tvwy6k1b2cm7\\\",\\\"s9n8stnpgh12\\\",\\\"g65gmolm3fa2\\\",\\\"tvwy12fhstbd\\\",\\\"s9n8123fjlx0\\\",\\\"s9n8de34bd9v\\\",\\\"3fceuvb2fhe4\\\",\\\"e534v0gh3fnp\\\",\\\"tvwy6kc3d3wy\\\",\\\"3ft93e5h3fop\\\",\\\"g6xzabd4e5st\\\",\\\"9wruk7p88rc3\\\",\\\"s9n812hj8rwx\\\",\\\"tvn82c5i7pfg\\\",\\\"tvn8wykm6lx0\\\",\\\"fhw0st6lg6o8\\\",\\\"4gde2dghd46j\\\",\\\"s9n8opd3ikpq\\\",\\\"e5b2a1xy5il7\\\",\\\"8r7o12km4fwy\\\",\\\"5ifhb2p8hja2\\\",\\\"3fqsrubdcei6\\\",\\\"3f3evxtv4g6l\\\",\\\"g6stlmd4g6i6\\\",\\\"tvn8xza1lmbc\\\",\\\"s9n8giprt912\\\",\\\"s9n8w0kmnpwx\\\",\\\"e5mnqs7p5i9w\\\",\\\"a2xylmln6lac\\\",\\\"8r2d5iuw45ru\\\",\\\"tvwybcxz4glm\\\",\\\"xy347o23a1w0\\\",\\\"9wkm5h7o4glm\\\",\\\"fhp86la1cers\\\",\\\"tvn8b2oqcde4\\\"],\\\"alg\\\":\\\"mix-ACSS\\\"},\\\"date\\\":\\\"201902061540\\\"}\",\"ttl\":1728000}"
    val path:Path= CouchbasePath(topic,ttl)
    dataWriter.writeOne(value, path)
    dataWriter.closeInstance()
  }


}




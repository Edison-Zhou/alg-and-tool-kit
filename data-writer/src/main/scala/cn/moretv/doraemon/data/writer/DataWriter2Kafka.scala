package cn.moretv.doraemon.data.writer

import cn.moretv.doraemon.common.path.{CouchbasePath, Path, RedisPath}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization


/**
  * Created by baozhiwang on 2018/6/11.
  */
class DataWriter2Kafka extends DataWriter {

  private val kafkaConnector: KafkaConnector = new KafkaSourceConnector
  implicit val formats = Serialization.formats(NoTypeHints)


  /**
    * 用来将DataFrame格式的数据以及存储组件的元信息（包含在path中）混合起来，写入Kafka
    *
    * @param df 两列，key，value。key用来作为redis或couchbase中的key，value为其中的值
    * @param path
    **/
  override def write(df: DataFrame, path: Path): Unit = {
    path match {
      case cp: CouchbasePath => {
        println("in CouchbasePath write")
        val ttl = cp.ttl
        val result = if (ttl < 0) df else df.withColumn("ttl", lit(ttl))
        kafkaConnector.write2Kafka(result.toJSON, cp.kafkaTopicName)
      }

      case rp: RedisPath => {
        println("in RedisPath write")
        val host = rp.host
        val port = rp.port
        val dbIndex = rp.dbIndex
        val ttl = if (rp.ttl < -1) -1 else rp.ttl
        val formatType = rp.convertFormatTypeEnum()
        val result = df.withColumn("host", lit(host)).withColumn("port", lit(port)).withColumn("dbIndex", lit(dbIndex)).withColumn("ttl", lit(ttl)).withColumn("formatType", lit(formatType))
        kafkaConnector.write2Kafka(result.toJSON, rp.kafkaTopicName)
      }

      case _ => throw new Exception("请传入正确的Path类,couchbasePath or redisPath")
    }
  }

  def writeOne(value: Map[String, Any], path: Path): Unit = {
    path match {
      case rp: RedisPath => {
        println("in RedisPath write")
        val host = rp.host
        val port = rp.port
        val dbIndex = rp.dbIndex
        val ttl = if (rp.ttl < -1) -1 else rp.ttl
        val formatType = rp.convertFormatTypeEnum()
        val result = value ++ Map("host" -> host, "port" -> port, "dbIndex" -> dbIndex, "ttl" -> ttl, "formatType" -> formatType)
        kafkaConnector.writeOne(Serialization.write(result), rp.kafkaTopicName)
      }

      case _ => throw new Exception("请传入正确的Path类, redisPath")
    }
  }

  /**
    * 写入单个字符串到kafka中
    * */
  def writeOne(value: String, path: Path): Unit = {
    path match {
     case cp: CouchbasePath => {
          println("in CouchbasePath write")
          kafkaConnector.writeOne(value, cp.kafkaTopicName)
        }
      case _ => throw new Exception("请传入正确的Path类,CouchbasePath")
    }
  }

  def closeInstance() : Unit = {
    kafkaConnector.closeInstance()
  }
}

package cn.moretv.doraemon.data.writer

import org.apache.spark.sql._

/**
  *
  * @author wang.baozhi 
  * @since 2018/6/26 下午2:34
  */
trait KafkaConnector extends Serializable{
  def write2Kafka(dataSet: Dataset[String],topicName:String)
  def writeOne(value: String, topicName:String)
  def closeInstance() : Unit

}

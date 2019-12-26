package cn.moretv.doraemon.data.writer

import java.util.{UUID, Properties}

import cn.moretv.doraemon.data.writer.util.EctdUtils
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.{ StringSerializer}
import org.apache.spark.sql.{Dataset}

/**
  *
  * @author wang.baozhi 
  * @since 2018/6/26 下午2:35
  *
  *  写入Kafka
  */
class KafkaSourceConnector extends KafkaConnector{

  final val BOOTSTRAP_SERVERS="bigdata-appsvr-130-1:9095," +
    "bigdata-appsvr-130-2:9095," +
    "bigdata-appsvr-130-3:9095," +
    "bigdata-appsvr-130-4:9095," +
    "bigdata-appsvr-130-5:9095," +
    "bigdata-appsvr-130-6:9095,"  +
    "bigdata-appsvr-130-7:9095," +
    "bigdata-appsvr-130-8:9095," +
    "bigdata-appsvr-130-9:9095"

  var producerInstance: Producer[String, String] = _

  def createProducerInstance(): Producer[String, String]= {
    val config: Properties = new Properties

    //beta for etcd rest api v2,保护，查看一段时间稳定性
    var bootstrapServers=""
    try{
      bootstrapServers = EctdUtils.getValue("kafka","bootstrap_servers")
    }catch {
      case e: Exception => println(e.printStackTrace())
        bootstrapServers=BOOTSTRAP_SERVERS
    }finally{
      if(!bootstrapServers.equalsIgnoreCase(BOOTSTRAP_SERVERS)){
        println("get bootstrap_servers from etcd,but not equal with BOOTSTRAP_SERVERS")
        bootstrapServers=BOOTSTRAP_SERVERS
      }
      config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers)
    }

    config.put(ProducerConfig.CLIENT_ID_CONFIG, this.getClass.getName+UUID.randomUUID().toString)
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    new KafkaProducer[String, String](config)
  }


  override def write2Kafka(dataSet: Dataset[String],topicName:String): Unit = {
    dataSet.foreachPartition(
      dataInEachPartition => {
        val createProducer = createProducerInstance
        dataInEachPartition.foreach(kafkaValue=>{
          val msg = new ProducerRecord[String,String](topicName,UUID.randomUUID().toString(),kafkaValue)
          createProducer.send(msg)
        })
        createProducer.close()
      }
    )
  }

  def writeOne(value: String, topicName:String): Unit = {
    if(producerInstance == null) {
      producerInstance = createProducerInstance
    }
    val msg = new ProducerRecord[String,String](topicName,UUID.randomUUID().toString(), value)
    producerInstance.send(msg)
  }

  def closeInstance() : Unit = {
    if(producerInstance != null) {
      producerInstance.close()
    }
    producerInstance = null
  }
}

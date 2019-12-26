package cn.moretv.doraemon.common.path

/**
  * Created by lituo on 2018/6/13.
  *
  * ttl为0，表示永不失效
  * 默认ttl为172800，表示key保留两天
  *
  * @param kafkaTopicName kafka topic名称
  * @param ttl couchbase过期时间，要求非负数
  */
case class CouchbasePath(kafkaTopicName:String,ttl: Integer=172800) extends Path {

  def getTopicName(): String ={
    this.kafkaTopicName
  }

}

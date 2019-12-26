package cn.moretv.doraemon.common.path

import cn.moretv.doraemon.common.enum.FormatTypeEnum

/**
  * Created by lituo on 2018/6/13.
  *
  * 默认ttl设置为-1，表示永不过期
  */
case class RedisPath(kafkaTopicName:String,host:String,port:Integer,dbIndex:Integer,ttl:Integer = -1,formatType:FormatTypeEnum.Value) extends Path {

  def convertFormatTypeEnum():String={
    formatType match {
      case FormatTypeEnum.KV=>"KV"
      case FormatTypeEnum.ZSET=>"ZSET"
      case FormatTypeEnum.HASH=>"HASH"

    }
  }


}

package cn.moretv.doraemon.data.writer.util

import com.fasterxml.jackson.databind.ObjectMapper


/**
  *
  * @author wang.baozhi 
  * @since 2018/7/5 下午1:44
  *
  * 引入jetcd 0.0.2版本有版本冲突，并引入大量的jar包。
  * rest api v3 版本本身为alpha版本。
  * 为了保证轻量和易用使用v2 rest api
  */

object EctdUtils {

  //将被nginx负载均衡取代
  val baseUrl="http://bigdata-appsvr-130-9:2379/v2/keys/doraemon/"

  //无需加锁，无共享变量
  def getValue(category:String,key:String): String ={
    val objectMapper = new ObjectMapper();
    val url=baseUrl+category+"/"+key
    val result = scala.io.Source.fromURL(url).mkString
    val jsonNode=objectMapper.readTree(result)
    val subJsonNode=jsonNode.get("node")
    val value=subJsonNode.get("value").asText()
    value
  }



  def main(args: Array[String]): Unit = {
    val BOOTSTRAP_SERVERS="bigdata-appsvr-130-1:9095," +
      "bigdata-appsvr-130-2:9095," +
      "bigdata-appsvr-130-3:9095," +
      "bigdata-appsvr-130-4:9095," +
      "bigdata-appsvr-130-5:9095," +
      "bigdata-appsvr-130-6:9095,"  +
      "bigdata-appsvr-130-7:9095," +
      "bigdata-appsvr-130-8:9095," +
      "bigdata-appsvr-130-9:9095"

    val etcdBootstrap= getValue("kafka","bootstrap_servers")
    println(etcdBootstrap)
    println(BOOTSTRAP_SERVERS)
    if(etcdBootstrap.equalsIgnoreCase(BOOTSTRAP_SERVERS)){
       println("equal")
    }else{
      println("not equal")
    }
  }

}

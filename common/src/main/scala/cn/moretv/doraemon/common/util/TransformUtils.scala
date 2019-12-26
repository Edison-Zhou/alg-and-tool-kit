package cn.moretv.doraemon.common.util


/**
  *
  * @author wang.baozhi 
  * @since 2018/7/13 下午1:08
  *
  *
  * 逐渐替换通用sdk，sdk_2.11
  *         <groupId>cn.whaley.bigdata</groupId>
            <artifactId>sdk_2.11</artifactId>
  *
  *暂时放在common中，后期合并到wdsdk中。各个用途的sdk以模块的形式在wdsdk。目前hbase sdk就是这样。
  * 防止引入大sdk，带入许多用不上的包，导致兼容性问题，以及打包冗余。
  *
  * wdsdk
  *    |
  *     - hbase sdk
  *     - 算法uid，vid转化等sdk
  *     - kafka sdk
  *     - redis sdk
  *     - hdfs sdk
  *      ....
  */
object TransformUtils {
  def transferSid(sid:String):Int={
    CodeIDOperator.codeToId(sid).toInt
  }

  def calcLongUserId(userId:String):Long={
    if(userId==null || userId.length != 32){
      -1l
    }else{
      userId2Long(userId)
    }
  }

  def userId2Long(s:String) = {
    var hash:Long = 0
    for(i<- 0 until s.length){
      hash = 47*hash + s.charAt(i).toLong
    }

    hash = math.abs(hash)

    if(hash<1000000000){
      hash = hash + 1000000000*(hash+"").charAt(0).toInt
    }
    hash
  }


  def main(args: Array[String]) {
    println(transferSid("tvwy9v7nmnvx"))
  }

}

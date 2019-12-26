package cn.moretv.doraemon.data.reader

import java.util.Properties

import cn.moretv.doraemon.common.path.MysqlPath
import cn.moretv.doraemon.data.util.MySqlOps
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
  * Created by guohao on 2018/6/27.
  */
private[reader] object MysqlReader {
  val LOG = LoggerFactory.getLogger(this.getClass)

  def read(mysqlPath:MysqlPath): DataFrame ={
    val ss = SparkSession.builder().enableHiveSupport().getOrCreate()
    //读取解析参数
    val host = mysqlPath.host.trim
    val port = mysqlPath.port
    val database = mysqlPath.database
    val tableName = mysqlPath.tableName
    val user = mysqlPath.user
    val password = mysqlPath.password
    val parField = mysqlPath.parField
    val selectFiled = mysqlPath.selectFiled
    val filterCondition = if(mysqlPath.filterCondition == null || mysqlPath.filterCondition.trim == ""){
      " 1 = 1 "
    }else{
      mysqlPath.filterCondition
    }
    val advancedSql = mysqlPath.advancedSql
    val url = s"jdbc:mysql://${host}:${port}/${database}?useUnicode=true&characterEncoding=utf-8&autoReconnect=true"
    val props = new Properties()
    props.setProperty("driver","com.mysql.jdbc.Driver")
    props.setProperty("url",url)
    props.setProperty("user",user)
    props.setProperty("password",password)
    //最大最小id
    val sqlMinMaxId = s"select min($parField),max($parField) from ${tableName}"
    val sqlInfo = s"select ${parField} from ${tableName} where ${parField} >= ? and ${parField} <= ?  and  ${filterCondition}"

    val mySqlOps = MySqlOps.apply(props)
    val (min,max) = mySqlOps.queryMaxMinIDBySql(sqlMinMaxId)
    val maxTotalNum = max - min
    val preDefNum = 10*10000
    //小于预定义数量忽略数据倾斜可能产生的问题
    val data = if(maxTotalNum < 10*10000){
      LOG.debug(s"table:${tableName} 数据量为:$maxTotalNum 小于预定义值:${preDefNum} 忽略数据倾斜问题")
      (min to max).toArray
    }else{
      //自动解决数据倾斜问题
      //预估分区数
      val partitionNum = getPartitionNum(maxTotalNum)
      MySqlOps.getJdbcRDD(ss.sparkContext,props,sqlMinMaxId,sqlInfo,partitionNum,rs => rs.getLong(1)).collect().sortBy(x=>x)
    }
    //获取每个分区的
    val predicates = getPredicates(data,parField,filterCondition)
    //当为全量字段时候，直接返回dataFrame
    val df = if(selectFiled(0).trim == "*"){
      ss.sqlContext.read.jdbc(url,tableName,predicates,props)
    }else{
      ss.sqlContext.read.jdbc(url,tableName,predicates,props).selectExpr(selectFiled:_*)
    }

    getFinalDF(df,advancedSql)
  }

  def getFinalDF(df:DataFrame,sql:String): DataFrame ={
    if(sql.contains("tmp") || sql.contains("TMP")){
      df.createOrReplaceTempView("tmp")
      df.sqlContext.sql(sql)
    }else{
      df
    }
  }

  /**
    * 获取条件分区
    * @param data
    * @param parField
    * @return
    */
  private def getPredicates(data:Array[Long],parField:String,filterContition:String): Array[String] ={
    val totalSize = data.length
    val partitionNum = getPartitionNum(totalSize)
    val batchSize = totalSize / partitionNum
    //按照batchSize分多个id区间，每个区间的记录数相同
    val arrayBuffer = new ArrayBuffer[String]
    if(totalSize> batchSize && batchSize!=0){
      val times = partitionNum
      var start = 0
      for(i<- 1 to times ){
        val startValue = data(start)
        val endValue = data(batchSize * i-1)
        arrayBuffer+=s"${parField} >=${startValue} and ${parField} <=${endValue} and ${filterContition}"
        start = batchSize *i
      }
      //余数
      val remain = totalSize - times * batchSize
      if(remain >0){
        val startValue = data(start)
        val endValue = data(totalSize -1)
        arrayBuffer+=s"${parField} >=${startValue} and ${parField} <=${endValue} and ${filterContition}"
      }
    }else{
      val startValue = data(0)
      val endValue = data(totalSize -1)
      arrayBuffer+=s"${parField} >=${startValue} and ${parField} <=${endValue} and ${filterContition}"
    }
    arrayBuffer.toArray
  }


  /**
    * 获取partition数 大小
    * @param totalSize
    * @return
    */
  private def getPartitionNum(totalSize:Long): Int ={
    val partitionNum = totalSize match {
      case i if i <= 10*10000 => 10
      case i if (i >= 10*10000 && i <= 50*10000)  => 20
      case i if (i >= 50*10000 && i <= 100*10000)  => 50
      case i if (i >= 100*10000 && i <= 500*10000)  => 100
      case i if (i >= 500*10000 && i <= 1000*10000)  => 200
      case i if (i >= 1000*10000 && i <= 10000*10000)  => 500
      case _=> 1000
    }
    partitionNum
  }

}

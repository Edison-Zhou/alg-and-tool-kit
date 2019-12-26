package cn.moretv.doraemon.data.reader

import cn.moretv.doraemon.common.path.HivePath
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by guohao on 2018/7/6.
  */
private[reader] object HiveReader {

  /**
    * 数据读取加载入口
    * @param path
    * @return
    */
  def read(path:HivePath): DataFrame ={
    val ss:SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    val sql = path.sql
    if(sql==null || "".equals(sql.trim)){
      throw new RuntimeException("请输入正确的sql ...")
    }
    ss.sqlContext.sql(sql)
  }
}

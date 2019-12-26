package cn.moretv.doraemon.data.reader

import cn.moretv.doraemon.common.enum.FileFormatEnum
import cn.moretv.doraemon.common.path.HdfsPath
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by guohao on 2018/6/21.
  */
private[reader] object HdfsReader {

  /**
    * 数据读取加载入口
    * @param hp
    * @return
    */
  def read(hp:HdfsPath):DataFrame = {
    val ss:SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    /**
      * 获取数据源路径
      */
    val paths = hp.getHdfsPathArray()
    if(paths.isEmpty){
      throw new Exception("未匹配到数据源读取路径")
    }
    println("detail path is:")
    paths.foreach(println)

    read(ss,paths,hp)
  }

  /**
    * 加载数据源返回dataFrame对象
    * 1.支持文件
    * 2.支持sql
    * @param ss
    * @param paths hdfs上数据源路径集合
    * @param hp
    * @return
    */
  protected def read(ss:SparkSession,paths:Array[String],hp:HdfsPath): DataFrame ={
    val df = getDF(ss,paths,hp.fileType)
    if(hp.sql == null){
      return df
    }

    if(!hp.fileType.equals(FileFormatEnum.PARQUET) && !hp.fileType.equals(FileFormatEnum.JSON)){
      throw new Exception("非parquet和json文件暂不支持sql")
    }
    return readWithSql(df,hp.sql)
  }

  /**
    * 加载数据源返回dataFrame对象
    *支持文件
    * @param ss
    * @param paths
    * @param inputFileType
    * @return 加载数据获取dataFrame
    */
  protected def getDF(ss:SparkSession,paths:Array[String],inputFileType:FileFormatEnum.Value): DataFrame ={
    inputFileType match {
      case FileFormatEnum.TEXT => ss.read.text(paths:_*)
      case FileFormatEnum.CSV => ss.read.format("csv").option("header","true").load(paths:_*)
      case FileFormatEnum.JSON => ss.read.json(paths:_*)
      case FileFormatEnum.PARQUET => ss.read.parquet(paths:_*)
      case _ => throw new Exception("目前暂不支持读取该类型文件")
    }
  }

  /**
    * 注册一个临时表以支持sql
    * @param df
    * @param sql
    * @return 返回dataFrame对象
    */
  protected def readWithSql(df:DataFrame,sql:String): DataFrame ={
    if(sql.contains("tmp") || sql.contains("TMP")){
      df.createOrReplaceTempView("tmp")
      return df.sqlContext.sql(sql)
    }
    throw new Exception(s"sql 表名必须是 tmp , 如:select id from tmp ")
  }



}

package cn.moretv.doraemon.common.path

import java.text.SimpleDateFormat
import java.util.Date

import cn.moretv.doraemon.common.constant.Constants
import cn.moretv.doraemon.common.enum.FileFormatEnum
import cn.moretv.doraemon.common.util.{DateUtils, HdfsUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

/**
  * Created by lituo on 2018/6/13.
  *
  */
case class HdfsPath(dr:DateRange, private val hdfsPath:String, fileType:FileFormatEnum.Value, sql:String) extends Path() {

  /**
    * 可配置化的路径
    * @param dr
    * @param hdfsPath 带有正则的路径 #{date}
    */
  def this(dr:DateRange,hdfsPath:String){
    this(dr,hdfsPath,FileFormatEnum.PARQUET,null)
  }

  /**
    *可配置化的路径+sql
    * @param dr
    * @param hdfsPath
    * @param sql 通过sql挑选需要的字段
    */
  def this(dr:DateRange,hdfsPath:String,sql:String){
    this(dr,hdfsPath,FileFormatEnum.PARQUET,sql)
  }

  /**
    * 标准路径
    * @param hdfsPath
    * @param fileType
    */
  def this(hdfsPath:String,fileType:FileFormatEnum.Value){
    this(null,hdfsPath,fileType,null)
  }

  /**
    * 标准路径+文件类型(json、parquet) + sql
    * @param hdfsPath
    * @param fileType
    * @param sql
    */
  def this(hdfsPath:String,fileType:FileFormatEnum.Value,sql:String){
    this(null,hdfsPath,fileType,sql)
  }

  /**
    * 读取parquet数据+sql
    * @param hdfsPath
    * @param sql
    */
  def this(hdfsPath:String,sql:String){
    this(null,hdfsPath,FileFormatEnum.PARQUET,sql)
  }

  /**
    * 读取parquet
    * @param hdfsPath
    */
  def this(hdfsPath: String) = {
    this(hdfsPath,FileFormatEnum.PARQUET)
  }

  /**
    * 一般在save中使用
    * @return 返回path路径
    */
  def getHdfsPath(): String ={
    if(this.hdfsPath.contains(Constants.INPUT_PATH_DATE_PATTERN)){
      throw new Exception(s"该路径为不合法路径 : ${this.hdfsPath}")
    }
    this.hdfsPath
  }

  /**
    * DataReader 使用
    * 1.初始化startDate,endDate值
    * 2.获取path路径
    * 3.过滤不存在path
    * @return 获取hdfs路径
    */
  def getHdfsPathArray():Array[String]={
    val conf = new Configuration()
    val fs = FileSystem.get(conf)

    val paths:Array[String] =
      this.hdfsPath.contains(Constants.INPUT_PATH_DATE_PATTERN) match {
        case true => {
          //0.判断传参是否正确
          if(this.dr == null){
            throw new Exception("输入路径为:"+this.hdfsPath+ " 包含#{date} ,请使用 new HdfsPath(dr:DateRange,hdfsPath:String) 方式读取")
          }
          //1.初始化startDate,endDate值
          if (dr.endDate == null && dr.startDate == null) {
            val df = new SimpleDateFormat(dr.dateFormat)
            val endDate = df.format(new Date())
            dr.endDate = endDate
            dr.startDate = DateUtils.getDateTimeWithOffset(df, endDate, -dr.numberDays)
            dr.endDate = DateUtils.getDateTimeWithOffset(df, endDate, -1)
          }
          //2.获取path路径
          dr.getDateArray().map(date => {
            this.hdfsPath.replace(Constants.INPUT_PATH_DATE_PATTERN, date)
          }).flatMap(path=>{
            val fileStatus = fs.globStatus(new org.apache.hadoop.fs.Path(path))
            fileStatus.map(fs=>{
              fs.getPath.toString
            }).toSeq
          }).filter(path=>{
            HdfsUtil.pathIsExist(path)
          })

        }
        case false => Array(this.hdfsPath)
      }
    paths
  }



}

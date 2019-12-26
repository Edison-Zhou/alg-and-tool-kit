package cn.moretv.doraemon.common.util

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by cheng_huan on 2017/2/9.
  * 用于日期相关的各种操作
  * 带分割符的日期用于字段操作
  * 不带分隔符的日期用于文件命名
  */
object DateUtils {
  /**
    * 用于获取昨天的日期(带分隔符)
    *
    * @return 昨天日期
    */
  def lastDayWithDelimiter:String = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DAY_OF_MONTH, -1)
    dateFormat.format(calendar.getTime)
  }

  /**
    * 用于获取当天的日期(带分隔符)
    *
    * @return 当天日期
    */
  def todayWithDelimiter:String = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance()
    dateFormat.format(calendar.getTime)
  }

  /**
    * 用于获取周期内最远一天的日期(带分隔符)
    *
    * @param numOfDays 周期的天数(周7，月30，季度90)
    * @return 周期内最远一天的日期
    */
  def farthestDayWithDelimiter(numOfDays:Int):String ={
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DAY_OF_MONTH, -numOfDays)
    dateFormat.format(calendar.getTime)
  }

  /**
    * 取某指定日期前numOfDay的日期
    *
    * @param referenceDay 指定日期
    * @param numOfDays 天数
    * @return 日期："yyyy-MM-dd"
    */
  def farthestDay2referenceWithDelimiter(referenceDay:String, numOfDays:Int):String ={
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance()
    val referenceDate = dateFormat.parse(referenceDay)
    calendar.setTime(referenceDate)
    calendar.add(Calendar.DAY_OF_MONTH, -numOfDays)
    dateFormat.format(calendar.getTime)
  }

  /**
    * 用于获取昨天的日期(不带分隔符)
    *
    * @return 昨天日期
    */
  def lastDayWithOutDelimiter:String = {
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DAY_OF_MONTH, -1)
    dateFormat.format(calendar.getTime)
  }

  /**
    * 用于获取当天的日期(不带分隔符)
    *
    * @return 当天日期
    */
  def todayWithOutDelimiter:String = {
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val calendar = Calendar.getInstance()
    dateFormat.format(calendar.getTime)
  }

  /**
    * 用于获取周期内最远一天的日期(不带分隔符)
    *
    * @param numOfDays 周期的天数(周7，月30，季度90)
    * @return 周期内最远一天的日期
    */
  def farthestDayWithOutDelimiter(numOfDays:Int):String ={
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DAY_OF_MONTH, -numOfDays)
    dateFormat.format(calendar.getTime)
  }

  /**
    * 用于打印时间，观察各子模块运行时间
    */
  def printTime: Unit = {
    val cal = Calendar.getInstance()
    val rightNowTime = cal.getTime
    println(rightNowTime)
  }

  /**
    * 获取系统当前时间，取小时并转换为Int
    *
    * @return hour
    */
  def hour: Int = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val calendar = Calendar.getInstance()
    val time = dateFormat.format(calendar.getTime)

    time.substring(11,13).toInt
  }

  /**
    * 用于获取时间戳
    *
    * @return 时间戳
    */
  def getTimeStamp:String = {
    val dateFormat = new SimpleDateFormat("yyyyMMddHHmm")
    val calendar = Calendar.getInstance()
    dateFormat.format(calendar.getTime)
  }

  /**
    * @param dateFormat
    * @param startDate 开始时间
    * @param endDate 结束时间
    * @return startDate->endDate之间的日期（包括startDate,endDate）
    */
  def getDateArray(dateFormat: String,startDate:String,endDate:String): Array[String] ={

    val arrayBuffer = new ArrayBuffer[String]()
    val df = new SimpleDateFormat(dateFormat)
    var date = startDate
    arrayBuffer += date
    while (!date.equals(endDate)){
      date = getDateTimeWithOffset(df,date,1)
      arrayBuffer += date
    }
    arrayBuffer.toArray
  }


  /**
    * @param df DateFormat
    * @param date
    * @param offset 偏移量
    * @return 输入日期的偏移日期(天偏移)
    */
  def getDateTimeWithOffset(df:SimpleDateFormat,date:String,offset:Int): String ={
    val calendar = Calendar.getInstance()
    calendar.setTime(df.parse(date))
    calendar.add(Calendar.DAY_OF_MONTH, offset)
    df.format(calendar.getTime)
  }








  def main(args: Array[String]): Unit = {
    val numberDays = -2
    val df = new SimpleDateFormat("yyyyMMdd")
    val startDate = df.format(new Date())
    val endDate = DateUtils.getDateTimeWithOffset(df,startDate,numberDays+1)
    println(endDate)
  }
}

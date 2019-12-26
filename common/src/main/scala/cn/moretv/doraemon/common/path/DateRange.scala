package cn.moretv.doraemon.common.path
import cn.moretv.doraemon.common.util.DateUtils

/**
  * Created by guohao on 2018/6/21.
  */
/**
  * 日期处理
  * @param dateFormat
  * @param startDate
  * @param endDate
  * @param numberDays
  */
case class DateRange (dateFormat:String,
                      var startDate:String,
                        var endDate:String,numberDays:Int) {
  /**
    *
    * @param dateFormat 时间格式化参数
    * @param startDate 开始时间
    * @param endDate   结束时间
    */

  def this(dateFormat:String, startDate:String,endDate:String){
      this(dateFormat,startDate,endDate,0)
  }

  /**
    *
    * @param dateFormat 时间格式化参数
    * @param numberDays 日期偏移量（以当前时间为基准，偏移），以天为单位
    */
  def this(dateFormat:String,
           numberDays:Int){
    this(dateFormat,null,null,numberDays)
  }

  /**
    * 获取时间集合
    * @return 返回时间集合
    */
  def getDateArray(): Array[String] ={
    if(dateFormat.length != startDate.length || startDate.length != endDate.length){
      throw new Exception(s"请输入正确的参数形式:${startDate} ,${endDate}")
    }
    if(startDate > endDate){
      throw new Exception(s"开始日期不能大于结束日期,startDate=$startDate,endDate=$endDate"  )
    }
    try {
      DateUtils.getDateArray(dateFormat,startDate,endDate)
    }catch {
      case e:Exception=>{
        throw e
      }
    }

  }

}

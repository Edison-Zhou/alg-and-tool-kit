package cn.moretv.doraemon.algorithm.hotVideo

import cn.moretv.doraemon.common.alg.AlgParameters
import cn.moretv.doraemon.common.path.HdfsPath

class HotVideoParameters extends AlgParameters {
  val algName: String = "HotVideo"

  /*
  * 筛选热门影片时，用以统计播放次数的评分阈值
  * */
  var scoreThreshold: Double = 0.5

  /*
  * 热门影片的数目
  * */
  var hotNum: Int = 480

  /*
  * 每日、每分钟和每小时等不同时间单位对应的微秒数,
  * 每日对应 1000 * 60 * 60 * 24
  * 每小时对应 1000 * 60 * 60
  * 每分钟对应 1000 * 60
  * 算法中默认以小时作为时间量度计算popularity, 可在biz中传值对应不同的
  * */
  var timeMeasurement : String = "hour"

  val millisecondPerTimeUnit: Long = timeMeasurement match {
    case "day" => 1000 * 60 * 60 * 24
    case "hour" => 1000 * 60 * 60
    case "minute" => 1000 * 60
  }


  /*
  * 影片列、评分列、播放次数列等对应的列名
  * */
  var videoColumn = "sid"
  var scoreColumn = "score"
  var countColumn = "count"
  var optimeColumn = "optime"
  var typeColumn = "content_type"
  var durationColumn = "duration"
  var popularityColumn = "popularity"


  /*指定传入热门影片的类型，默认是全部类型*/
  var contentType = "all"

  def validation(): String ={
    "ok"
  }

  def updateFromJsonString(jsonString: String): Boolean = {
    false
  }

  def updateFromMap(paramMap: Map[String, String]): Boolean = {
    false
  }

  def loadFromHdfs(hdfsPath: HdfsPath): Boolean = {
    false
  }

  def saveToHdfs(hdfsPath: HdfsPath): Boolean = {
    false
  }
}

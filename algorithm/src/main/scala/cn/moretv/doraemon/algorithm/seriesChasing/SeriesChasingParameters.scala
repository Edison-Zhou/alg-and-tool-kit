package cn.moretv.doraemon.algorithm.seriesChasing

import cn.moretv.doraemon.common.alg.AlgParameters
import cn.moretv.doraemon.common.path.HdfsPath

/**
  * Created by cheng_huan on 2018/6/26.
  */
class SeriesChasingParameters extends AlgParameters{
  override val algName: String = "SeriesChasing"
  var thresholdNumOfDaysContinuouslyWatched = 2
  var thresholdOfWatch2UpdateRatio = 1.0
  var paramArray: Array[(String, Int, Double, Int)] = Array(("tv", 2, 0.5, 7),
    ("kids", 3, 0.5, 7), ("zongyi", 2, 0.5, 8), ("jilu", 3, 0.5, 7), ("comic", 3, 0.5, 7))

  def validation(): String ={
    "Exception"
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

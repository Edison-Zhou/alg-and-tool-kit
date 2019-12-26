package cn.moretv.doraemon.algorithm.similar.last4offlineindicator

import cn.moretv.doraemon.common.alg.AlgParameters
import cn.moretv.doraemon.common.path.HdfsPath

/**
  *
  * @author wang.baozhi 
  * @since 2018/7/30 上午11:17 
  */
class Last4OfflineIndicatorParameter  extends AlgParameters{
  val algName: String = "Last4OfflineIndicator"
  var numOfDaysRetainAnImpression = 60
  var numOfDaysUserWatchedLongVideos = 180
  var thresholdScoreOfUserPrefer = 0.5
  var nDaysAgoEnd = 15

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

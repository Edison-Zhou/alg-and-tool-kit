package cn.moretv.doraemon.algorithm.similar.latest

import cn.moretv.doraemon.common.alg.AlgParameters
import cn.moretv.doraemon.common.path.HdfsPath

/**
  *
  * @author wang.baozhi 
  * @since 2018/7/30 上午11:17 
  */
class SimilarLatestParameter  extends AlgParameters{
  val algName: String = "SimilarLatest"
  var numOfDaysRetainAnImpression = 1
  var numOfDaysUserWatchedLongVideos = 300
  var thresholdScoreOfUserPrefer = 0.5

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

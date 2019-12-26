package cn.moretv.doraemon.algorithm.longVideoCluster

import cn.moretv.doraemon.common.alg.AlgParameters
import cn.moretv.doraemon.common.path.HdfsPath

/**
  *
  * @author wang.baozhi 
  * @since 2018/7/26 下午5:25 
  */
class Cluster4OfflineIndicatorParameters extends AlgParameters{
  val algName: String = "Cluster4OfflineIndicator"
  var nDayAgoEnd = 15
  var numOfPastBasedLongVideoCluster4Filter = 80
  var numOfLongVideoCluster4Recommend = 100
  var numOfDaysUserWatchedLongVideos = 180

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

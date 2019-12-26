package cn.moretv.doraemon.algorithm.kMeans

import cn.moretv.doraemon.common.alg.AlgParameters
import cn.moretv.doraemon.common.path.HdfsPath

/**
  *
  * @author wang.baozhi 
  * @since 2018/10/8 上午8:46
  */
class KMeansParameters extends AlgParameters{
  val algName: String = "kMeans"
  var videoType = "movie"
  var kNum = 1000
  var numIter = 100

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

package cn.moretv.doraemon.algorithm.randomCluster

import cn.moretv.doraemon.common.alg.AlgParameters
import cn.moretv.doraemon.common.path.HdfsPath

/**
  * Created by cheng_huan on 2018/8/13.
  */
class RandomClusterParameters extends AlgParameters {
  override val algName: String = "randomCluster"
  var numOfCluster = 40
  var numOfVideo = 1

  def validation(): String ={
    "OK"
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

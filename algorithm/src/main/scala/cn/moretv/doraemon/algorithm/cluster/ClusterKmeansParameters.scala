package cn.moretv.doraemon.algorithm.cluster

import cn.moretv.doraemon.common.alg.AlgParameters
import cn.moretv.doraemon.common.path.HdfsPath

class ClusterKmeansParameters extends AlgParameters{
  val algName: String = "ClusterKmeansModel"

  var clusterNums = Array(100, 200, 400, 600, 800, 1000, 1200)
  var clusterUnionNum = 1024
//  var clusterNums = Array(10, 20, 40)
  var filterMinSimilarity = 0.9

  var filterClusterMinSize = 5

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

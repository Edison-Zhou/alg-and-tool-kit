package cn.moretv.doraemon.algorithm.similar.cluster

import cn.moretv.doraemon.common.alg.AlgParameters
import cn.moretv.doraemon.common.path.HdfsPath

class SimilarClusterParameters extends AlgParameters {
  val algName: String = "SimilarCluster"

  /*
  * 取前多少个节目
  * */
  var topN: Int = 0

  var preferTableUserColumn = "userid"

  var preferTableClusterColumn = "cluster"

  var clusterDetailTableContentColumn = "sid"

  var clusterDetailTableClusterColumn = "cluster"

  var outputUserColumn = "user"

  var outputItemColumn = "item"

  def validation(): String = {
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

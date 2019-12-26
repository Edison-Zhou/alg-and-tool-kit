package cn.moretv.doraemon.algorithm.similar.vector

import cn.moretv.doraemon.common.alg.AlgParameters
import cn.moretv.doraemon.common.enum.DistanceType
import cn.moretv.doraemon.common.path.HdfsPath

class SimilarVectorParameters extends AlgParameters {
  val algName: String = "SimilarVector"

  /*
  * 是否稀疏存储，默认为非稀疏存储
  * */
  var isSparse = false
  /**
    * 最少取前多少个节目
    */
  var topN: Int = 100
  /**
    * 保留的最少相似度(不包含)
    */
  var minSimilarity: Double = 0
  var firstColumn: String = "sid"
  var itemColumn: String = "item"
  var similarityColumn: String = "similarity"
  /*
  * 距离计算类型，默认为cos函数，暂时不提供修改
  * */
  private var distanceType: DistanceType.Value = DistanceType.Cosine

  def validation(): String = {
    if (minSimilarity < 0) {
      "最小相似度不能小于0"
    } else {
      "ok"
    }
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

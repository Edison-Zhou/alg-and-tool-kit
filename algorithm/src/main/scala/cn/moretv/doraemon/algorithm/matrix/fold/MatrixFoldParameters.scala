package cn.moretv.doraemon.algorithm.matrix.fold

import cn.moretv.doraemon.common.alg.AlgParameters
import cn.moretv.doraemon.common.path.HdfsPath


class MatrixFoldParameters extends AlgParameters {

  val algName: String = "matrixFold"

  var idX:String = "idx"

  var idY:String = "idy"

  var score:String = "score"

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

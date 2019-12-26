package cn.moretv.doraemon.algorithm.matrix.multiplication

import cn.moretv.doraemon.common.alg.AlgParameters
import cn.moretv.doraemon.common.path.HdfsPath

class MatrixMultiplicationParameters extends AlgParameters {
  val algName: String = "MatrixMultiplication"

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

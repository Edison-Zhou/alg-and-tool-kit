package cn.moretv.doraemon.algorithm.als

import cn.moretv.doraemon.common.alg.AlgParameters
import cn.moretv.doraemon.common.path.HdfsPath

class AlsParameters extends AlgParameters {
  val algName: String = "ALS"
  var alpha:Double = 40
  var checkpointInterval:Int = 10
  var implicitPrefs:Boolean = true
  var rank:Int = 30
  var maxIter:Int = 50
  var numUserBlocks:Int = 1000
  var numItemBlocks:Int = 1000
  var regParam:Double = 2.0
  var nonnegative: Boolean = false

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

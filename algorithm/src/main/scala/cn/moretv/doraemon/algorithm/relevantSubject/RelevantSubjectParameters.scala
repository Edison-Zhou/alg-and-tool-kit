package cn.moretv.doraemon.algorithm.relevantSubject

import cn.moretv.doraemon.common.alg.AlgParameters
import cn.moretv.doraemon.common.path.HdfsPath

/**
  * Created by cheng_huan on 2018/8/22.
  */
class RelevantSubjectParameters extends AlgParameters{
  override val algName: String = "relevantSubject"
  var subjectNum: Int = 10

  def validation(): String = {
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

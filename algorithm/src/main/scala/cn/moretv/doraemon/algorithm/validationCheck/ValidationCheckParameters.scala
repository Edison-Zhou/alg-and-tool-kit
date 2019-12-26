package cn.moretv.doraemon.algorithm.validationCheck

import cn.moretv.doraemon.common.alg.AlgParameters
import cn.moretv.doraemon.common.path.HdfsPath

/**
  * Created by cheng_huan on 2018/6/22.
  */
class ValidationCheckParameters extends AlgParameters {
  override val algName: String = "ValidationCheck"

  var whiteOrBlackList: String = "whiteList"

  var userOrItem: String = null

  def validation(): String = {
    if (!List("whiteList", "blackList").contains(whiteOrBlackList)) {
      "whiteOrBlackList is not valid"
    } else if (!List("user", "item").contains(userOrItem)) {
      "userOrItem is not valid"
    } else "ok"
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

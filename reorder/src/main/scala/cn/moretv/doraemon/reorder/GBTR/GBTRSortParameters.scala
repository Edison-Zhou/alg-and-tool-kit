package cn.moretv.doraemon.reorder.GBTR

import cn.moretv.doraemon.common.alg.AlgParameters
import cn.moretv.doraemon.common.path.HdfsPath

/**
  * Created by cheng_huan on 2019/7/3.
  */
class GBTRSortParameters extends AlgParameters{
  override val algName: String = "GBTRSort"
  var featureSize = 1024
  var recommendSize = 60
  var modelPath = "unspecified"

  override def validation(): String = {
    "ok"
  }

  override def loadFromHdfs(hdfsPath: HdfsPath): Boolean = false

  override def updateFromJsonString(jsonString: String): Boolean = false

  override def saveToHdfs(hdfsPath: HdfsPath): Boolean = false

}

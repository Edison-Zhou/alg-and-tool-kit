package cn.moretv.doraemon.reorder.linearRegression

import cn.moretv.doraemon.common.alg.AlgParameters
import cn.moretv.doraemon.common.path.HdfsPath

/**
  * Created by cheng_huan on 2019/7/2.
  */
class LinearRegressionSortParameters extends AlgParameters{
  override val algName: String = "LinearRegressionSort"

  var featureSize: Int = 1024
  var recommendSize: Int = 100
  var modelPath: String = "unspecified"

  override def validation(): String = {
    "ok"
  }

  override def saveToHdfs(hdfsPath: HdfsPath): Boolean = false

  override def updateFromJsonString(jsonString: String): Boolean = false

  override def loadFromHdfs(hdfsPath: HdfsPath): Boolean = false

}

package cn.moretv.doraemon.algorithm.testalg

import cn.moretv.doraemon.common.alg.AlgParameters
import cn.moretv.doraemon.common.path.HdfsPath

/**
  * Created by lituo on 2018/6/15.
  */
class TestParameter extends AlgParameters {
  override val algName = "test"

  /**
    * 要排序的列
    */
  var columnName: String = _

  /**
    * 是否降序，默认true
    */
  var desc: Boolean = true

  override def updateFromJsonString(jsonString: String): Boolean = {
    false
  }

  override def loadFromHdfs(hdfsPath: HdfsPath): Boolean = {
    false
  }

  override def saveToHdfs(hdfsPath: HdfsPath): Boolean = {
    false
  }

  /**
    * 参数内容验证
    *
    * @return 如果成功返回"ok"或者空，失败返回错误信息
    */
  override def validation() = {
    if (columnName == null) {
      "未设置参数columnName"
    } else {
      "ok"
    }
  }
}

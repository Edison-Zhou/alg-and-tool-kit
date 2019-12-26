package cn.moretv.doraemon.reorder.logistic

import cn.moretv.doraemon.common.alg.Model
import org.apache.spark.mllib.classification.LogisticRegressionModel

/**
  * Created by liu.qiang on 2018/6/28.
  */
class LogisticModel extends Model {

  val modelName: String = "logistic"

  var modelResult: LogisticRegressionModel = _


}

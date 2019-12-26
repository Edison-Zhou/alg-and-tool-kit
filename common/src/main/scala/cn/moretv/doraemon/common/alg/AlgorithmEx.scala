package cn.moretv.doraemon.common.alg

/**
  * Created by lituo on 2018/6/12.
  */
trait AlgorithmEx extends Algorithm {

  var modelInput: Map[String, Model] = _

  final def initInputModel(modelMap: Map[String, Model]): Unit = {
    modelInput = modelMap
  }

}

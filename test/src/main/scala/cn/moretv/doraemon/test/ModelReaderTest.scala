package cn.moretv.doraemon.test

import cn.moretv.doraemon.common.enum.{EnvEnum, ProductLineEnum}
import cn.moretv.doraemon.reorder.mix.RecommendMixModel

/**
  * Created by lituo on 2018/8/13.
  */
object ModelReaderTest extends BaseClass{

  implicit val productLine = ProductLineEnum.medusa

  override def execute(): Unit = {
    val recommendMixModel = new RecommendMixModel
    recommendMixModel.load()

    recommendMixModel.mixResult.show()

  }
}

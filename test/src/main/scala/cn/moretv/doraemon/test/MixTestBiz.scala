package cn.moretv.doraemon.test

import cn.moretv.doraemon.common.enum.{EnvEnum, ProductLineEnum}
import cn.moretv.doraemon.reorder.mix.{MixModeEnum, RecommendMixAlgorithm, RecommendMixModel, RecommendMixParameter}

/**
  * Created by baozhiwang on 2018/6/11.
  */
object MixTestBiz extends BaseClass {

  implicit val productLine = ProductLineEnum.medusa

  override def execute(): Unit = {

    //替代dataReader
    val sqlC = sqlContext
    import sqlC.implicits._

    val df1 =  Seq(
      ("a", "b", 0.1),
      ("a", "c", 0.3),
      ("a", "d", 3.0),
      ("a1", "b", 0.1),
      ("a1", "c", 0.3),
      ("a1", "d", 3.0)
    ).toDF("u", "s", "score")

    val df2 =  Seq(
      ("a", "e", 0.1),
      ("a", "f", 0.2),
      ("a", "g", 0.8),
      ("a1", "d", 0.2),
      ("a1", "g", 0.1),
      ("a1", "c", 0.3),
      ("a1", "g", 0.1),
      ("a1", "h", 0.3),
      ("a1", "i", 0.2),
      ("a1", "d", 3.0),
      ("a2", "d", 3.0)
    ).toDF("u", "s", "score")

    //算法
    val mixAlg: RecommendMixAlgorithm = new RecommendMixAlgorithm

    val param = mixAlg.getParameters.asInstanceOf[RecommendMixParameter]
    param.recommendUserColumn = "u"
    param.recommendItemColumn = "s"
    param.scoreColumn = "score"
    param.recommendNum = 4
    param.mixMode = MixModeEnum.RATIO
    param.ratio = Array(1,2)

    mixAlg.initInputData(Map(mixAlg.INPUT_DATA_KEY_PREFIX + "1" -> df1 , mixAlg.INPUT_DATA_KEY_PREFIX + "2" -> df2))

    mixAlg.run()

    //替代dataWriter
//    mixAlg.getOutputModel.asInstanceOf[RecommendMixModel].mixResult.show()

//    mixAlg.getOutputModel.save()

//    mixAlg.getOutputModel.output("mixTest", "mixResult")
    //modelWriter

//    ModelWriter.write(testAlg.getOutputModel, testAlg.getOutputModel.getModelPath("test"))

  }
}

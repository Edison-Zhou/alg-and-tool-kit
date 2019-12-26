package cn.moretv.doraemon.test

import cn.moretv.doraemon.common.enum.ProductLineEnum
import cn.moretv.doraemon.reorder.mix.{MixModeEnum, RecommendMixAlgorithm, RecommendMixParameter}

/**
  * Created by baozhiwang on 2018/6/11.
  */
object MixTestBiz1 extends BaseClass {

  implicit val productLine = ProductLineEnum.medusa

  override def execute(): Unit = {

    //替代dataReader
    val sqlC = sqlContext
    import sqlC.implicits._

    val df1 =  Seq(
      ("a", "b1"),
      ("a", "c1"),
      ("a", "d1"),
      ("a1", "b1"),
      ("a1", "c1"),
      ("a1", "d1")
    ).toDF("u", "s")

    val df2 =  Seq(
      ("a", "e2"),
      ("a1", "d2"),
      ("a1", "g2"),
      ("a1", "c2"),
      ("a1", "g2"),
      ("a1", "h2"),
      ("a1", "i2"),
      ("a1", "d2"),
      ("a2", "d2")
    ).toDF("u", "s")

    val df3 =  Seq(
      ("a", "e3"),
      ("a", "f3"),
      ("a", "g3"),
      ("a1", "d3"),
      ("a1", "g3"),
      ("a1", "c3"),
      ("a1", "g3"),
      ("a1", "h3"),
      ("a1", "i3"),
      ("a1", "d3"),
      ("a2", "d3")
    ).toDF("u", "s")

    val df4 =  Seq(
      ("a", "e4"),
      ("a", "f4"),
      ("a", "g4"),
      ("a1", "d4"),
      ("a1", "g4"),
      ("a1", "c4"),
      ("a1", "g4"),
      ("a1", "h4"),
      ("a1", "i4"),
      ("a1", "d4"),
      ("a2", "d4")
    ).toDF("u", "s")
    //算法
    val mixAlg: RecommendMixAlgorithm = new RecommendMixAlgorithm

    val param = mixAlg.getParameters.asInstanceOf[RecommendMixParameter]
    param.recommendUserColumn = "u"
    param.recommendItemColumn = "s"
//    param.scoreColumn = "score"
    param.recommendNum = 12
    param.mixMode = MixModeEnum.RATIO
    param.ratio = Array(1,2,1,2)

    mixAlg.initInputData(Map(mixAlg.INPUT_DATA_KEY_PREFIX + "1" -> df1 , mixAlg.INPUT_DATA_KEY_PREFIX + "2" -> df2,
      mixAlg.INPUT_DATA_KEY_PREFIX + "3" -> df3, mixAlg.INPUT_DATA_KEY_PREFIX + "4" -> df4))

    mixAlg.run()

    //替代dataWriter
//    mixAlg.getOutputModel.asInstanceOf[RecommendMixModel].mixResult.show()

    //mixAlg.getOutputModel.save()

//    mixAlg.getOutputModel.output("mixTest", "mixResult")
    //modelWriter

//    ModelWriter.write(testAlg.getOutputModel, testAlg.getOutputModel.getModelPath("test"))

  }
}

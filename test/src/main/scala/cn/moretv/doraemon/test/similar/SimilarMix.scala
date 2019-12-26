package cn.moretv.doraemon.test.similar

import cn.moretv.doraemon.algorithm.matrix.fold.{MatrixFoldAlgorithm, MatrixFoldModel, MatrixFoldParameters}
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.FormatTypeEnum
import cn.moretv.doraemon.common.path.{HdfsPath, RedisPath}
import cn.moretv.doraemon.data.writer.{DataPack, DataPackParam, DataWriter2Hdfs, DataWriter2Kafka}
import cn.moretv.doraemon.reorder.mix.{MixModeEnum, RecommendMixAlgorithm, RecommendMixModel, RecommendMixParameter}
import cn.moretv.doraemon.test.BaseClass

/**
  * 相似影片推荐结果融合
  * Created by lituo on 2018/7/20.
  */
object SimilarMix extends BaseClass {
  override def execute(): Unit = {

    //读取
    val contentTypeList = List("movie", "tv", "zongyi", "comic", "kids", "jilu")

    contentTypeList.foreach(contentType => {

      val word2VecDf = DataReader.read(new HdfsPath(s"/ai/project/detail/similar/test/word2vec/$contentType/Latest"))
      val tagDf = DataReader.read(new HdfsPath(s"/ai/project/detail/similar/test/tag/$contentType/Latest"))
      val defaultDf = DataReader.read(new HdfsPath(s"/ai/project/detail/similar/test/default/$contentType/Latest"))

      //算法
      val mixAlg: RecommendMixAlgorithm = new RecommendMixAlgorithm

      val param = mixAlg.getParameters.asInstanceOf[RecommendMixParameter]
      param.recommendUserColumn = "sid"
      param.recommendItemColumn = "item"
      param.scoreColumn = "similarity"
      param.recommendNum = 60
      param.mixMode = MixModeEnum.STACKING
      param.outputOriginScore = false

      mixAlg.initInputData(
        Map(mixAlg.INPUT_DATA_KEY_PREFIX + "1" -> word2VecDf,
          mixAlg.INPUT_DATA_KEY_PREFIX + "2" -> tagDf,
          mixAlg.INPUT_DATA_KEY_PREFIX + "3" -> defaultDf)
      )

      mixAlg.run()

      //输出到hdfs
      val mixResult = mixAlg.getOutputModel.asInstanceOf[RecommendMixModel].mixResult
      new DataWriter2Hdfs().write(mixResult, new HdfsPath("/ai/project/detail/similar/test/mix/" + contentType))

      //数据格式转换
      val foldAlg = new MatrixFoldAlgorithm
      val foldParam = foldAlg.getParameters.asInstanceOf[MatrixFoldParameters]
      foldParam.idX = "sid"
      foldParam.idY = "item"
      foldParam.score = "similarity"

      foldAlg.initInputData(Map(foldAlg.INPUT_DATA_KEY -> mixResult))
      foldAlg.run()

      val foldResult = foldAlg.getOutputModel.asInstanceOf[MatrixFoldModel].matrixFold

      //输出到redis
      val dataPackParam = new DataPackParam
      dataPackParam.format = FormatTypeEnum.ZSET
      dataPackParam.zsetAlg = "mix"
      val outputDf = DataPack.pack(foldResult, dataPackParam)

      val dataWriter = new DataWriter2Kafka
      val topic = "redis-moretv-topic-test"
      val host = "bigtest-cmpt-129-204"
      val port = 6379
      val dbIndex = 1
      val ttl = 8640000
      val formatType = FormatTypeEnum.ZSET

      val path = RedisPath(topic, host, port, dbIndex, ttl, formatType)
      dataWriter.write(outputDf, path)
    })

  }
}

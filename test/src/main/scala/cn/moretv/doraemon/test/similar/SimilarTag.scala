package cn.moretv.doraemon.test.similar

import cn.moretv.doraemon.algorithm.similar.vector.{SimilarVectorAlgorithm, SimilarVectorModel, SimilarVectorParameters}
import cn.moretv.doraemon.algorithm.validationCheck.{ValidationCheckAlgorithm, ValidationCheckModel, ValidationCheckParameters}
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.path.{HdfsPath, MysqlPath}
import cn.moretv.doraemon.data.writer.DataWriter2Hdfs
import cn.moretv.doraemon.test.{BaseClass, ConfigHelper}
import cn.whaley.sdk.utils.TransformUDF

/**
  * 基于标签相似度的相似影片推荐
  * Updated by lituo on 2018/7/18.
  */
object SimilarTag extends BaseClass {
  def execute(): Unit = {

    TransformUDF.registerUDFSS

    val contentTypeList = List("movie", "tv", "zongyi", "comic", "kids", "jilu")

    //读入节目对应的标签的数据
    val tagSidPath: HdfsPath = new HdfsPath("/data_warehouse/dw_normalized/videoTagFeature/current",
      "select videoSid as sid, tagFeatures as vector from tmp")
    val tagSidDF = DataReader.read(tagSidPath)
      .selectExpr("transformSid(sid) as sid", "vector").persist()

    contentTypeList.foreach(contentType => {
      //有效影片的数据
      val validSidPath: MysqlPath = ConfigHelper.getMysqlPath(contentType + "_valid_sid")
      val validSidDF = DataReader.read(validSidPath)
        .selectExpr("transformSid(sid) as sid")


      //数据的有效性检查
      val validAlg: ValidationCheckAlgorithm = new ValidationCheckAlgorithm()
      val validPara = validAlg.getParameters.asInstanceOf[ValidationCheckParameters]
      validPara.userOrItem = "item"

      val validDataMap = Map(validAlg.INPUT_DATA_KEY -> tagSidDF, validAlg.INPUT_CHECKLIST_KEY -> validSidDF)
      validAlg.initInputData(validDataMap)
      validAlg.run()

      val validTagSidDF = validAlg.getOutputModel.asInstanceOf[ValidationCheckModel].checkedData

      //相似度计算
      val similarAlg: SimilarVectorAlgorithm = new SimilarVectorAlgorithm()
      val similarPara: SimilarVectorParameters = similarAlg.getParameters.asInstanceOf[SimilarVectorParameters]
      similarPara.isSparse = true
      similarPara.topN = 60
      val similarDataMap = Map(similarAlg.INPUT_DATA_KEY -> validTagSidDF)
      similarAlg.initInputData(similarDataMap)
      similarAlg.run()
      //得到模型结果
      val recomendResultDF = similarAlg.getOutputModel.asInstanceOf[SimilarVectorModel].matrixData
      //结果输出到HDFS
      new DataWriter2Hdfs().write(recomendResultDF, new HdfsPath("/ai/project/detail/similar/test/tag/" + contentType))
    })

  }
}

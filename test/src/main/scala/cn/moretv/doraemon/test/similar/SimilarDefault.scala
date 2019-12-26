package cn.moretv.doraemon.test.similar

import cn.moretv.doraemon.algorithm.similar.cluster.{SimilarClusterAlgorithm, SimilarClusterModel, SimilarClusterParameters}
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.path.{HdfsPath, MysqlPath}
import cn.moretv.doraemon.data.writer.DataWriter2Hdfs
import cn.moretv.doraemon.test.{BaseClass, ConfigHelper}
import cn.whaley.sdk.utils.TransformUDF

/**
  * 相似影片默认推荐
  * Updated by lituo on 2018/7/16
  */
object SimilarDefault extends BaseClass {

  def execute(): Unit = {

    TransformUDF.registerUDFSS

    val contentTypeList = List("tv", "zongyi", "comic", "kids", "jilu")

    contentTypeList.foreach(contentType => {

      val hotDf = DataReader.read(
        new HdfsPath(
          "/ai/dw/moretv/rankings/hot/Latest",
          s"select transformSid(sid) as sid, '$contentType' as cluster from tmp where content_type = '$contentType'"
        ))

      val recommendDF = if(contentType != "movie")
        hotDf
      else
        hotDf.union(DataReader.read(ConfigHelper.getMysqlPath("movie_editor_recommend"))
          .selectExpr("transformSid(sid) as sid", "cluster"))

      //有效影片的数据
      val validSidPath: MysqlPath = ConfigHelper.getMysqlPath(s"${contentType}_valid_sid")
      val validSidDF = DataReader.read(validSidPath)
        .selectExpr("transformSid(sid) as sid", s"'$contentType' as cluster")

      //调用
      val similarAlg: SimilarClusterAlgorithm = new SimilarClusterAlgorithm()
      val similarClusterPara = similarAlg.getParameters.asInstanceOf[SimilarClusterParameters]
      similarClusterPara.preferTableUserColumn = "sid"
      similarClusterPara.clusterDetailTableContentColumn = "sid"
      similarClusterPara.outputUserColumn = "sid"
      similarClusterPara.topN = 70

      val dataMap = Map(similarAlg.INPUT_DATA_KEY_PREFER -> validSidDF,
        similarAlg.INPUT_DATA_KEY_CLUSTER_DETAIL -> recommendDF)
      similarAlg.initInputData(dataMap)

      similarAlg.run()
      //得到模型结果
      val recommendResultDF = similarAlg.getOutputModel.asInstanceOf[SimilarClusterModel].matrixData

      //结果输出到HDFS
      new DataWriter2Hdfs().write(recommendResultDF, new HdfsPath(s"/ai/project/detail/similar/test/default/$contentType"))
    })
  }
}

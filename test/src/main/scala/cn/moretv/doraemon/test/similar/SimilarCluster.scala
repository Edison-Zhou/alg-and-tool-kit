package cn.moretv.doraemon.test.similar

import cn.moretv.doraemon.algorithm.cluster.{ClusterKmeansAlgorithm, ClusterKmeansModel, ClusterKmeansParameters}
import cn.moretv.doraemon.algorithm.similar.cluster.{SimilarClusterAlgorithm, SimilarClusterModel, SimilarClusterParameters}
import cn.moretv.doraemon.algorithm.validationCheck.{ValidationCheckAlgorithm, ValidationCheckModel, ValidationCheckParameters}
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.FileFormatEnum
import cn.moretv.doraemon.common.path.{HdfsPath, MysqlPath}
import cn.moretv.doraemon.data.writer.DataWriter2Hdfs
import cn.moretv.doraemon.test.{BaseClass, ConfigHelper}
import cn.whaley.sdk.utils.TransformUDF
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions._

/**
  * 基于聚类的影片相似度
  * Updated by lituo on 2018/7/23
  */
object SimilarCluster extends BaseClass {
  def execute(): Unit = {
    val ss = spark
    import ss.implicits._

    TransformUDF.registerUDFSS

    val contentTypeList = List("movie", "tv", "zongyi", "comic", "kids", "jilu")

    contentTypeList.foreach(contentType => {
      //读入节目对应的Word2Vec数据
      val word2VecPath: HdfsPath = new HdfsPath(s"/ai/dw/moretv/base/word2vec/item/Latest/${contentType}Features.txt", FileFormatEnum.TEXT)
      val word2VecSidDF = DataReader.read(word2VecPath)
        .rdd.map(line => line.getString(0).split(",")).
        map(e => (e(0).toInt, e.takeRight(128))).
        map(e => (e._1, Vectors.dense(e._2.map(x => x.toDouble))))
        .toDF("sid", "vector")

      //有效影片的数据
      val validSidPath: MysqlPath = ConfigHelper.getMysqlPath(s"${contentType}_valid_sid")
      val validSidDF = DataReader.read(validSidPath)
        .selectExpr("transformSid(sid) as sid")

      //数据的有效性检查
      val validAlg: ValidationCheckAlgorithm = new ValidationCheckAlgorithm()
      val validPara = validAlg.getParameters.asInstanceOf[ValidationCheckParameters]
      validPara.userOrItem = "item"
      val validDataMap = Map(validAlg.INPUT_DATA_KEY -> word2VecSidDF, validAlg.INPUT_CHECKLIST_KEY -> validSidDF)
      validAlg.initInputData(validDataMap)
      validAlg.run()
      val validTagSidDF = validAlg.getOutputModel.asInstanceOf[ValidationCheckModel].checkedData

      //调用聚类方法
      val clusterAlg: ClusterKmeansAlgorithm = new ClusterKmeansAlgorithm()
      val clusterKmeansPara = clusterAlg.getParameters.asInstanceOf[ClusterKmeansParameters]
      val vectorDataMap = Map(clusterAlg.INPUT_DATA_KEY_VECTOR -> validTagSidDF)
      clusterAlg.initInputData(vectorDataMap)
      clusterAlg.run()
      val clusterDF = clusterAlg.getOutputModel.asInstanceOf[ClusterKmeansModel]

      //读入cluster的数据
      val recommendDF = clusterDF.matrixData.persist()

      new DataWriter2Hdfs().write(recommendDF, new HdfsPath(s"/ai/project/detail/similar/test/clusterdetail/$contentType"))


      //调用
      val similarAlg: SimilarClusterAlgorithm = new SimilarClusterAlgorithm()
      val similarClusterPara = similarAlg.getParameters.asInstanceOf[SimilarClusterParameters]
      similarClusterPara.topN = 70
      similarClusterPara.preferTableUserColumn = "item"
      similarClusterPara.clusterDetailTableContentColumn = "item"
      similarClusterPara.outputUserColumn = "sid"

      val dataMap = Map(similarAlg.INPUT_DATA_KEY_CLUSTER_DETAIL -> recommendDF,
        similarAlg.INPUT_DATA_KEY_PREFER -> recommendDF)

      similarAlg.initInputData(dataMap)
      similarAlg.run()
      //得到模型结果
      val recommendResultDF = similarAlg.getOutputModel.asInstanceOf[SimilarClusterModel].matrixData
      //结果输出到HDFS
      new DataWriter2Hdfs().write(recommendResultDF, new HdfsPath(s"/ai/project/detail/similar/test/cluster/$contentType"))
      recommendDF.unpersist()
    })
  }
}

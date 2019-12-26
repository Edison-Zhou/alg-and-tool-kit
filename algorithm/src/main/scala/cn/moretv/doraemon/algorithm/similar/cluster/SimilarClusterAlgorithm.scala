package cn.moretv.doraemon.algorithm.similar.cluster

import cn.moretv.doraemon.common.alg.Algorithm
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Updated by lituo on 20180717
  * 基于类别的相似推荐
  * prefer表中是等待推荐的用户（或节目）偏好的分类
  * clusterDetail表中是各分类下包含的内容
  */
class SimilarClusterAlgorithm extends Algorithm {

  /*
  * 用户偏好数据，主要格式为[userid,cluster] 或者 [sid,cluster]
  * */
  val INPUT_DATA_KEY_PREFER = "prefer"

  /*
  * 类别对应的详细数据
  * */
  val INPUT_DATA_KEY_CLUSTER_DETAIL = "clusterDetail"

  //输入输出属性定义
  val algParameters: SimilarClusterParameters = new SimilarClusterParameters()
  val modelOutput: SimilarClusterModel = new SimilarClusterModel()

  /*
  * 进行参数初始化设置
  * */
  override def beforeInvoke(): Unit = {

  }

  /*
  * 算子核心处理逻辑
  * */
  override def invoke(): Unit = {
    val ss: SparkSession = SparkSession.builder().getOrCreate()

    val clusterColumnName = "temp_col_cluster"

    val preferDF = dataInput(INPUT_DATA_KEY_PREFER)
      .selectExpr(algParameters.preferTableUserColumn + " as " + algParameters.outputUserColumn,
        algParameters.preferTableClusterColumn + " as " + clusterColumnName)
      .dropDuplicates()
    val clusterDetailDF = dataInput(INPUT_DATA_KEY_CLUSTER_DETAIL)
      .selectExpr(algParameters.clusterDetailTableContentColumn + " as " + algParameters.outputItemColumn,
        algParameters.clusterDetailTableClusterColumn + " as " + clusterColumnName)
      .dropDuplicates()

    val recommendData = preferDF.join(clusterDetailDF, preferDF(clusterColumnName) === clusterDetailDF(clusterColumnName), "leftouter")
      .drop(clusterColumnName)
      .filter(expr(algParameters.outputItemColumn + " is not null"))

    val result = if (algParameters.topN <= 0) {
      recommendData
    } else {
       val TEMP_SCORE = "temp_col_score"
       val TEMP_RANK = "temp_col_rank"
      //随机排序，并且取topN
      recommendData.withColumn(TEMP_SCORE, rand())
        .withColumn(TEMP_RANK, row_number().over(Window.partitionBy(algParameters.outputUserColumn).orderBy(col(TEMP_SCORE).desc))) //编号
        .filter(TEMP_RANK + " <= " + algParameters.topN) //过滤
        .drop(TEMP_SCORE, TEMP_RANK)
    }

    modelOutput.matrixData = result
  }

  /*
  * 清理阶段
  * */
  override def afterInvoke(): Unit = {

  }
}

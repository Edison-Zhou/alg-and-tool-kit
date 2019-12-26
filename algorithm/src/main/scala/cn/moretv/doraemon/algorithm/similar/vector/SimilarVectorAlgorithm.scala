
package cn.moretv.doraemon.algorithm.similar.vector

import cn.moretv.doraemon.algorithm.similar.SparseVectorComputation
import cn.moretv.doraemon.common.alg.Algorithm
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vectors}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, rand, row_number}

/**
  * Updated by lituo on 2018.07.18
  */
class SimilarVectorAlgorithm extends Algorithm {


  val INPUT_DATA_KEY = "input"

  /**
    * 相似算子的参数列表
    */
  val algParameters: SimilarVectorParameters = new SimilarVectorParameters()

  /**
    * 模型结果数据
    */
  val modelOutput: SimilarVectorModel = new SimilarVectorModel()

  /*
  * 进行参数初始化设置
  * */
  override def beforeInvoke(): Unit = {

  }

  /**
    * 本算子主要实现基于节目的维度数据，计算相似度
    *
    * @return
    */
  override def invoke(): Unit = {
    val ss: SparkSession = SparkSession.builder().getOrCreate()
    import ss.implicits._

    val inputData = dataInput(INPUT_DATA_KEY)

    val rdd = {
      if (algParameters.isSparse) { //稀疏向量
      val inputVector = inputData.rdd.map(x => {(x.getString(0), x.getAs[SparseVector](1))}).repartition(50)
        val cartesian = inputVector cartesian inputVector
        val a = cartesian.map(e => {
          val movieSid1 = e._1._1
          val movieSid2 = e._2._1
          val movieVector1 = e._1._2
          val movieVector2 = e._2._2
          val similarity = SparseVectorComputation.vectorCosineSimilarity(movieVector1, movieVector2)
          (movieSid1, movieSid2, similarity)
        })
        a
      } else {    //稠密向量
      val inputVector = inputData.rdd.map(x => {(x.getString(0), x.getAs[DenseVector](1))}).repartition(50)
        val cartesian2 = inputVector cartesian inputVector
        val a = cartesian2.map(e => {
          val movieSid11 = e._1._1
          val movieSid22 = e._2._1
          val movieVector11 = e._1._2
          val movieVector22 = e._2._2
          val similarity = SparseVectorComputation.vectorCosineSimilarity2(movieVector11, movieVector22)
          (movieSid11, movieSid22, similarity)
        })
        a
      }
    }.filter(e => e._1 != e._2 && e._3 > algParameters.minSimilarity)

    val result = rdd.toDF(algParameters.firstColumn, algParameters.itemColumn, algParameters.similarityColumn)

    //取topN
    val resultDf = if (algParameters.topN <= 0) {
      result
    } else {
      val TEMP_SCORE = "temp_col_score"
      val TEMP_RANK = "temp_col_rank"
      result.withColumn(TEMP_SCORE, col(algParameters.similarityColumn))
        .withColumn(TEMP_RANK, row_number().over(Window.partitionBy(algParameters.firstColumn).orderBy(col(TEMP_SCORE).desc))) //编号
        .filter(TEMP_RANK + " <= " + algParameters.topN) //过滤
        .drop(TEMP_SCORE, TEMP_RANK)
        .coalesce(500)
    }

    modelOutput.matrixData = resultDf
  }

  /**
    * 数据清理工作
    *
    * @return
    */
  override def afterInvoke(): Unit = {

  }

}

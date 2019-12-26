package cn.moretv.doraemon.algorithm.matrix.fold

import cn.moretv.doraemon.common.alg.{AlgParameters, Algorithm}
import org.apache.spark.sql.functions._

/**
  * Created by baozhiwang on 2018/6/21.
  *
  * 矩阵折叠操作
  *
  * 1.ALS相关
  *  RDD[(uid,sid,score)] fold to RDD[uid,Array[(sid,score)]]
  * 2.相似度相关
  * RDD[(sid,sid,score)] fold to RDD[sid,Array[(sid,score)]]
  *
  * 目前期望使用DF为统一接口的数据结构
  */
class MatrixFoldAlgorithm extends Algorithm{
  val INPUT_DATA_KEY = "input"

  //unfolded fields


  //fold to fields
  private val idxAfterFold:String = "idx"
  private val idyScoreArray:String = "idyScoreArray"



  override protected val modelOutput: MatrixFoldModel = new MatrixFoldModel

  override protected val algParameters: MatrixFoldParameters = new MatrixFoldParameters

  override protected def invoke(): Unit = {
    val inputData = dataInput(INPUT_DATA_KEY)
    val idX = algParameters.idX
    val idY = algParameters.idY
    val score = algParameters.score
    modelOutput.matrixFold=inputData.groupBy(idX).agg(collect_list(struct(idY,score))).toDF(idxAfterFold,idyScoreArray)
    //modelOutput.matrixFold.schema.printTreeString()
    //modelOutput.matrixFold.show
  }

  override protected def beforeInvoke(): Unit = {

  }

  override protected def afterInvoke(): Unit = {

  }

}

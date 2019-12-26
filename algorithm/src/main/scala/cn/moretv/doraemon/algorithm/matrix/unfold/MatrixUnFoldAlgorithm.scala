package cn.moretv.doraemon.algorithm.matrix.unfold

import cn.moretv.doraemon.common.alg.{Algorithm, AlgParameters}
import org.apache.spark.sql.functions._

/**
  * Created by baozhiwang on 2018/6/21.
  * 矩阵展开操作
  *
  * 1.ALS相关
  * RDD[(uid,Array[(sid,score)])] unfold to RDD[(uid,sid,score)]
  * 2.相似度相关
  * RDD[(sid,Array[(sid,score)])] unfold to RDD[(sid,sid,score)]
  *
  * 目前期望使用DF为统一接口的数据结构
  */
class MatrixUnFoldAlgorithm extends Algorithm{
  val INPUT_DATA_KEY = "input"

  //folded fields
  val idBeforeUnFold:String = "idx"
  val idyScoreArray:String = "idyScoreArray"

  //unfold to fields
  private val idX:String = "idx"
  private val idY:String = "idy"
  private val score:String = "score"

  override protected val modelOutput: MatrixUnFoldModel = new MatrixUnFoldModel

  override protected val algParameters: AlgParameters = new MatrixUnFoldParameters

  override protected def invoke(): Unit = {
    val inputData = dataInput(INPUT_DATA_KEY)
    val dfSource = inputData.select(inputData(idBeforeUnFold),explode(inputData(idyScoreArray)))
    println("1--"+dfSource.schema)
    modelOutput.matrixUnFold = dfSource.select(idX,"col._1", "col._2").toDF(idX,idY,score)
    println("2--"+modelOutput.matrixUnFold.schema)
    modelOutput.matrixUnFold.show()
  }

  override protected def beforeInvoke(): Unit = {

  }

  override protected def afterInvoke(): Unit = {

  }
}

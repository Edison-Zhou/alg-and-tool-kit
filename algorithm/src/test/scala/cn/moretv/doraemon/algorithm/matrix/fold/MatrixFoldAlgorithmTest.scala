package cn.moretv.doraemon.algorithm.matrix.fold

import org.apache.spark.sql._
import org.junit.{Before, Test}

/**
  * Created by baozhiwang on 2018/6/22.
  *
  * 矩阵折叠操作
  *
  * 1.ALS相关
  * RDD[(uid,sid,score)] fold to RDD[uid,Array[(sid,score)]]
  * 2.相似度相关
  * RDD[(sid,sid,score)] fold to RDD[sid,Array[(sid,score)]]
  *
  * 目前期望使用DF为统一接口的数据结构
  */
class MatrixFoldAlgorithmTest {
  val ss = SparkSession
    .builder
    .appName("MatrixFoldAlgorithmTest")
    .master("local")
    .getOrCreate()

  val sc = ss.sparkContext
  val sqlContext = ss.sqlContext
  var matrixSource: DataFrame = null
  var dataSource: DataFrame = null
  val alg = new MatrixFoldAlgorithm

  @Before
  def initData(): Unit = {
    import sqlContext.implicits._
    dataSource = Seq(
      (11l, 91, 0.1),
      (12l, 92, 0.2),
      (12l, 93, 0.3)
    ).toDF("idx", "idy", "score")
    dataSource.show()
    dataSource.schema.printTreeString()
    matrixSource = dataSource
  }

  @Test
  def run(): Unit = {
    val dataSourceMap = Map(alg.INPUT_DATA_KEY -> matrixSource)
    alg.initInputData(dataSourceMap)
    alg.run()
  }

}

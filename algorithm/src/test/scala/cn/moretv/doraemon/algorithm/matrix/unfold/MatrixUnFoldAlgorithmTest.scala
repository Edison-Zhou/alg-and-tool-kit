package cn.moretv.doraemon.algorithm.matrix.unfold

import org.apache.spark.sql._
import org.junit.{Before, Test}

/**
  * Created by baozhiwang on 2018/6/22.
  * 矩阵展开操作
  *
  * 1.ALS相关
  * RDD[uid,Array[(sid,score)]] unfold to RDD[(uid,sid,score)]
  * 2.相似度相关
  * RDD[sid,Array[(sid,score)]] unfold to RDD[(sid,sid,score)]
  *
  * 目前期望使用DF为统一接口的数据结构
  */
class MatrixUnFoldAlgorithmTest {
  val ss = SparkSession
    .builder
    .appName("MatrixUnFoldAlgorithmTest")
    .master("local")
    .getOrCreate()

  val sc = ss.sparkContext
  val sqlContext = ss.sqlContext
  var matrixSource:DataFrame = null
  var dataSource:DataFrame = null
  val alg=new MatrixUnFoldAlgorithm

  @Before
  def initData():Unit={
    import sqlContext.implicits._
    dataSource=Seq(
      (11l, 91, 0.1),
      (12l, 92, 0.2),
      (12l, 93, 0.3)
    ).toDF("uid", "sid", "score")
     matrixSource=dataSource.rdd.map(e=>(e.getLong(0),(e.getInt(1),e.getDouble(2)))).groupByKey.map(e=>(e._1,e._2.toArray)).toDF(alg.idBeforeUnFold,alg.idyScoreArray)
     matrixSource.show()
     matrixSource.schema.printTreeString()
  }

  @Test
  def run(): Unit = {
    val dataSourceMap=Map(alg.INPUT_DATA_KEY->matrixSource)
    alg.initInputData(dataSourceMap)
    alg.run()
  }

}

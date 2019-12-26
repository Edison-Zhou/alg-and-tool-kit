package cn.moretv.doraemon.algorithm.als

import cn.moretv.doraemon.common.alg.Algorithm
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SparkSession

/**
  * Created by cheng_huan on 2018/6/14.
  * 算法说明：主要实现基于[userid,itemid,score]类型的数据结构进行ALS分解，分解得到的UV矩阵存入到Model类中
  */
class AlsAlgorithm extends Algorithm{

  //数据Map的Key定义
  val INPUT_DATA_KEY = "input"
  //输入输出属性定义
  val algParameters: AlsParameters = new AlsParameters()
  val modelOutput: AlsModel = new AlsModel()
  //内部数据传递
  private val als = new ALS
  private val scoreColName = "rating"
  private val featureColName = "features"
  private var userColName:String = "user"
  private var itemColName:String = "item"
  private var paramMap: ParamMap = _

  //进行参数初始化设置
  override def beforeInvoke(): Unit = {
    /*dataInput.get(INPUT_DATA_KEY) match {
        case None => throw new IllegalArgumentException("未设置输入数据")
    }
*/
    //此处需要设定原数据中列标题数据
    val inputData = dataInput(INPUT_DATA_KEY)
    if (inputData.columns.length >= 2) {
      userColName = inputData.columns(0)
      itemColName = inputData.columns(1)
    }

    paramMap = ParamMap(als.alpha -> algParameters.alpha).
      put(als.checkpointInterval, algParameters.checkpointInterval).
      put(als.implicitPrefs, algParameters.implicitPrefs).
      put(als.userCol, userColName).
      put(als.itemCol, itemColName).
      put(als.rank, algParameters.rank).
      put(als.maxIter, algParameters.maxIter).
      put(als.numUserBlocks, algParameters.numUserBlocks).
      put(als.numItemBlocks, algParameters.numItemBlocks).
      put(als.regParam, algParameters.regParam).
      put(als.nonnegative, algParameters.nonnegative)
  }

  //算子核心处理逻辑
  override def invoke(): Unit =
  {
    val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    import ss.implicits._
    val sc: SparkContext = ss.sparkContext
    sc.setCheckpointDir("/ai/tmp/ALS/checkpoint")

    //数据格式预处理
    val inputData = dataInput(INPUT_DATA_KEY)
    val dataSet1 = inputData.rdd.map(e => (e.getLong(0), (e.getString(1), e.getDouble(2).toFloat)))
      .groupByKey().zipWithIndex().map(e => (e._1, e._2.toInt))
      .persist(StorageLevel.DISK_ONLY)
    //RDD[(uid, Iterable(sid, score)), uidIndex]

    val dataSet2 = dataSet1.flatMap((x => x._1._2.map(y => (y._1, (x._2, y._2)))))
      .groupByKey().zipWithIndex().map(e => (e._1, e._2.toInt))
      .persist(StorageLevel.MEMORY_AND_DISK)
    //RDD[(sid, Iterable(uidIndex, score)), sidIndex]

    val trainData = dataSet2.flatMap(x=> x._1._2.map(y=>(y._1, x._2, y._2)))
      .toDF(userColName, itemColName, scoreColName)
    //DataFrame[uidIndex, sidIndex, score]

    val uidIndexMap = dataSet1.map(x => (x._2, x._1._1))
    val sidIndexMap = dataSet2.map(x => (x._2, x._1._1))

    //模型训练及结果输出
    val alsModelSpark = als.fit(trainData, paramMap)
    modelOutput.matrixU = alsModelSpark.userFactors.rdd
      .map(e => (e.getInt(0), e.getSeq[Float](1).map(e => e.toDouble))).join(uidIndexMap)
      .map(e => (e._2._2, e._2._1)).toDF(userColName, featureColName)
    modelOutput.matrixV = alsModelSpark.itemFactors.rdd
      .map(r => (r.getInt(0), r.getSeq[Float](1).map(e => e.toDouble))).join(sidIndexMap)
      .map(e => (e._2._2, e._2._1)).toDF(itemColName, featureColName)
  }

  //清理阶段
  override def afterInvoke(): Unit ={

  }
}





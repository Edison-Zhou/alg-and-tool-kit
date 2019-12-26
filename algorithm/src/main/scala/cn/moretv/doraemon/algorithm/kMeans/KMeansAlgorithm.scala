package cn.moretv.doraemon.algorithm.kMeans

import cn.moretv.doraemon.algorithm.util.VectorUtils
import cn.moretv.doraemon.common.alg.Algorithm
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.storage.StorageLevel

/**
  *
  * @author wang.baozhi 
  * @since 2018/7/26 下午5:24 
  */
class KMeansAlgorithm  extends Algorithm{
  override protected val algParameters: KMeansParameters = new KMeansParameters()
  override protected val modelOutput: KMModel = new KMModel()

  //数据Map的Key定义
  //活跃用户
  val INPUT_ACTIVE_USER = "activeUser"
  //用户ALS
  val INPUT_USER_ALS = "userALS"


  override protected def beforeInvoke(): Unit = {
    dataInput.get(INPUT_ACTIVE_USER) match {
      case None => throw new IllegalArgumentException("未设置输入数据:活跃用户")
      case _ =>
    }
    dataInput.get(INPUT_USER_ALS) match {
      case None => throw new IllegalArgumentException("未设置输入数据:用户ALS")
      case _ =>
    }
  }

  override protected def invoke(): Unit = {
    /**
      * ---------------------------------------------获取数据，并转换---------------------------------------------
      */
    //1.读取用户ALS隐式矩阵，得到活跃用户的隐式向量
    val activeUserInfo=dataInput(INPUT_ACTIVE_USER).rdd.map(r => (r.getLong(0), 1))
    val userDataByALS=dataInput(INPUT_USER_ALS).rdd.map(r => (r.getLong(0), r.getSeq[Double](1))).
      join(activeUserInfo).map(r => (r._1, r._2._1))

    //2.将用户数据转化为电影稀疏向量
    val userInfo2Vector = userDataByALS.map(r => (r._1, VectorUtils.denseVector2Sparse(r._2)))
      .map(_._2.compressed)
      .persist(StorageLevel.MEMORY_AND_DISK)

    /**
      *------------------------------------计算---------------------------------------------
      * */
    //训练KMeans模型
    val kMeans = new KMeans().setK(algParameters.kNum).setMaxIterations(algParameters.numIter)
    val model = kMeans.run(userInfo2Vector)
    modelOutput.kMeansModel=model
  }

  override protected def afterInvoke(): Unit = {

  }

}

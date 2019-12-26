package cn.moretv.doraemon.algorithm.randomCluster

import cn.moretv.doraemon.common.alg.Algorithm
import cn.moretv.doraemon.common.util.ArrayUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
  * Created by cheng_huan on 2018/8/13.
  */
class RandomClusterAlgorithm extends Algorithm{
  //输入数据Map的Key定义
  val INPUT_VIDEOCLUSTER_KEY = "videoCluster"
  val INPUT_VALIDVIDEO_KEY = "validVideo"

  //输入输出属性定义
  val algParameters: RandomClusterParameters = new RandomClusterParameters()
  val modelOutput: RandomClusterModel = new RandomClusterModel()

  //进行参数初始化设置
  override def beforeInvoke(): Unit =  {
    dataInput.get(INPUT_VIDEOCLUSTER_KEY) match {
      case None => throw new IllegalArgumentException("未设置输入数据")
      case _ =>
    }
    dataInput.get(INPUT_VALIDVIDEO_KEY) match {
      case None => throw new IllegalArgumentException("未设置输入数据")
      case _ =>
    }
  }

  override def invoke(): Unit = {
    val videoCluster = dataInput(INPUT_VIDEOCLUSTER_KEY)
      .rdd.map(r => (r.getInt(0), r.getSeq[String](1)))
      .flatMap(e => e._2.map(x => (x, e._1)))
    val validVideos = dataInput(INPUT_VALIDVIDEO_KEY)
      .rdd.map(r => (r.getString(0), 1))
      .persist(StorageLevel.MEMORY_AND_DISK)

    val validVideoCluster = videoCluster.join(validVideos)
      .map(e => (e._2._1, e._1))
      .groupByKey()
      .map(e => (e._1, e._2.toArray))
      .persist(StorageLevel.MEMORY_AND_DISK)

    val clusterIndexes = validVideoCluster.map(e => e._1).collect()
    val cluster2videoMap = validVideoCluster.collectAsMap()

    val output = validVideos.map(e => {
      val randomClusterIndexes = ArrayUtils.randomTake(clusterIndexes, algParameters.numOfCluster)
      var result = Array.empty[String]
      randomClusterIndexes.foreach(x => {
        result = result ++: ArrayUtils.randomTake(cluster2videoMap.getOrElse(x, Array.empty[String]), algParameters.numOfVideo)
      })
      (e._1, result)
    })

    val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    import ss.implicits._
    modelOutput.randomClusterData = output.toDF("sid", "randomClusterSids")
  }

  //清理阶段
  override def afterInvoke(): Unit ={

  }
}

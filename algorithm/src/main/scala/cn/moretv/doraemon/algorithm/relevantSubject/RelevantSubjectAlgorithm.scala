package cn.moretv.doraemon.algorithm.relevantSubject

import cn.moretv.doraemon.common.alg.Algorithm
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ArrayBuffer

/**
  * Created by cheng_huan on 2018/8/22.
  */
class RelevantSubjectAlgorithm extends Algorithm {
  //数据Map的Key定义
  val INPUT_VIDEOID2SUBJECT_KEY = "videoId2subject"
  val INPUT_AVAILABLESUBJECT_KEY = "availableSubject"
  val INPUT_AVAILABLEVIDEO_KEY = "availabeVideo"
  val INPUT_VIDEOFEATURE_KEY = "videoFeature"

  //输入输出属性定义
  val algParameters: RelevantSubjectParameters = new RelevantSubjectParameters
  val modelOutput: RelevantSubjectModel = new RelevantSubjectModel

  //进行参数初始化设置
  override def beforeInvoke(): Unit =  {
    dataInput.get(INPUT_AVAILABLESUBJECT_KEY) match {
      case None => throw new IllegalArgumentException("未设置输入数据")
      case _ =>
    }
    dataInput.get(INPUT_AVAILABLEVIDEO_KEY) match {
      case None => throw new IllegalArgumentException("未设置输入数据")
      case _ =>
    }
    dataInput.get(INPUT_VIDEOFEATURE_KEY) match {
      case None => throw new IllegalArgumentException("未设置输入数据")
      case _ =>
    }
    dataInput.get(INPUT_VIDEOID2SUBJECT_KEY) match {
      case None => throw new IllegalArgumentException("未设置输入数据")
      case _ =>
    }
  }

  override protected def invoke(): Unit = {
    val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    import ss.implicits._

    val videoId2subject = dataInput(INPUT_VIDEOID2SUBJECT_KEY)
    //DF("item_sid", "subject_code", "item_contentType")

    val availableSubject = dataInput(INPUT_AVAILABLESUBJECT_KEY)
    //DF("content_type", "code")

    val availableVideo = dataInput(INPUT_AVAILABLEVIDEO_KEY)
    //DF("sid")

    val videoFeatureDF = dataInput(INPUT_VIDEOFEATURE_KEY)
    //DF("sid", "feature")

    val availableSubject2VideoInfo = videoId2subject.rdd
      .map(r => (r.getString(0), r.getString(1), r.getString(2)))
      .filter(r => r._2.contains(r._3)).map(r => (r._2, r._1))
    //RDD(subject_code, item_sid)

    // 获取本类长视频
    val eachAvailableVideoInfo = availableVideo.rdd
      .map(r => (r.getString(0), 1))

    // 获取数据库提供的可用专题号(id, (code, score))
    val subjectCodeByMysql = availableSubject.rdd.map(r => (r.getString(0), r.getString(1)))
      .map(r => (r._2, r._1)).join(availableSubject2VideoInfo)
      .map(r => (r._2._2, (r._1, r._2._1))).
      join(eachAvailableVideoInfo).
      map(r => (r._1, (r._2._1._1, 100)))

    // 获取Stephen提供的128维向量
    val videoFeature = videoFeatureDF.rdd.map(r => (r.getString(0), r.getSeq[Double](1))).
      join(eachAvailableVideoInfo).map(r => (r._1, r._2._1))

    // 计算每个专题的128维向量
    val subjectVector = subjectCodeByMysql.
      map(r => (r._1, r._2._1)).join(videoFeature).map(r => (r._2._1, r._2._2.toArray)).
      reduceByKey((x, y) => {
        val size = x.length
        val sequence = new ArrayBuffer[Double]()
        for(i <- 0 until size)
          sequence += (x(i) + y(i))
        sequence.toArray
      }).collectAsMap()

    val subjectVectorMap = ss.sparkContext.broadcast(subjectVector)

    // 求视频和专题的相似度
    val subjectCodeByCosineSimilarity = videoFeature.map(r => (r._1, r._2.toArray)).
      mapPartitions(partition => {
        val value = subjectVectorMap.value
        val length = value.size
        partition.map(r => {
          val videoSid = r._1
          val videoVector = r._2
          val subjectArrayByScore = new Array[(String, Double)](length)
          var j = 0
          for (i <- value.keys){
            val score = cosineSimilarity(videoVector, value.getOrElse(i, null))
            subjectArrayByScore.update(j, (i, score))
            j = j+1
          }
          (videoSid, subjectArrayByScore)
        })
      }).flatMap(r => r._2.map(x => (r._1, x)))

    val output = subjectCodeByMysql.map(r => (r._1, (r._2._1, r._2._2.toDouble))).
      repartition(200).
      union(subjectCodeByCosineSimilarity).groupByKey().
      map(r => {
        val videoId = r._1
        val subject = r._2.toArray.sortBy(-_._2).take(algParameters.subjectNum).map(_._1)
        (videoId, subject)
      }).toDF("sid", "subjectArray")

    modelOutput.subjectData = output
  }

  override protected def afterInvoke(): Unit = {

  }

  /**
    * 用于计算两个稀疏向量的内积
    * @param V1 稀疏向量1
    * @param V2 稀疏向量2
    * @return 内积
    */
  private def dotProduct(V1: Array[Double], V2: Array[Double]):Double = {
    var result:Double = 0.0
    require(V1.size == V2.size, "The two SparseVector have different sizes!")

    for(i <- 0 until V1.size )
      result += V1(i) * V2(i)

    result
  }

  /**
    * 用于计算两个稀疏向量的余弦相似度
    * @param V1 稀疏向量1
    * @param V2 稀疏向量2
    * @return 余弦相似度
    */
  private def cosineSimilarity(V1: Array[Double], V2: Array[Double]): Double = {
    val value1 = dotProduct(V1, V2)
    val value2 = math.sqrt(dotProduct(V1, V1))
    val value3 = math.sqrt(dotProduct(V2, V2))
    if(value2 == 0 || value3 == 0)
      0
    else
      value1/(value2 * value3)
  }
}

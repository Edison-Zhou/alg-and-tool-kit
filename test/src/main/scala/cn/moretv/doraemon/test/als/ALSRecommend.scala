package cn.moretv.doraemon.test.als

import cn.moretv.doraemon.algorithm.als.AlsModel
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.path.HdfsPath
import cn.moretv.doraemon.common.util.DateUtils
import cn.moretv.doraemon.data.writer.DataWriter2Hdfs
import cn.moretv.doraemon.test.BaseClass
import cn.moretv.doraemon.test.constant.PathConstants
import cn.moretv.doraemon.test.util.BizUtils
import cn.whaley.sdk.algorithm.{TopN, VectorFunctions}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 使用ALS模型进行推荐
  * 为首页今日推荐模块&会员看看模块提供基础数据
  *
  * @author wang.baozhi
  * @since 2018/8/6 下午1:23
  */
object ALSRecommend extends BaseClass{

  /**
    * 构造根据得分将视频递减排序的类
    * */
  class OrderByScore extends Ordering[(Int,Float)]{
    override def compare(x:(Int,Float),y:(Int,Float))={
      if(x._2 > y._2) -1 else if(x._2 == y._2) 0 else 1
    }
  }
  /**
    * 用ALS的结果进行推荐
    *
    * @param userFactorsRaw ALS训练的用户特征
    * @param itemFactorsRaw ALS训练的视频特征
    * @param topN 推荐视频数目
    * @param videoDataDF 有效视频数据
    * @param weighedVideos 需要加权的视频
    * @param userWatchedDF 用户看过的视频
    * @param userExposedDF 曝光给用户过的视频
    * @return DataFrame[(uid, sid，score)]
    */
  def ALSRecommend(userFactorsRaw:DataFrame,
                   itemFactorsRaw:DataFrame,
                   topN: Int,
                   videoDataDF: DataFrame,
                   weighedVideos: Map[Int, Double],
                   userWatchedDF: DataFrame,
                   userExposedDF: DataFrame):DataFrame = {
    val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    val sc: SparkContext = ss.sparkContext
    import ss.implicits._
    //过滤有效视频
    val itemFactors=itemFactorsRaw.join(videoDataDF,itemFactorsRaw("item")===videoDataDF("sid")).select(videoDataDF("sid"),itemFactorsRaw("features"))
    BizUtils.getDataFrameInfo(itemFactors,"itemFactors")

    //加权
    val itemFactorsRDD = itemFactors.rdd.
      map(x => (x.getInt(0),x.getSeq[Double](1).toArray)).
      map(x => {
        val sid = x._1
        val weight = weighedVideos.getOrElse(sid, 1.0)

        (sid, x._2.map(y => (weight * y).toFloat))
      })

    //将item特征向量作为广播变量
    val bcItemVectors = sc.broadcast(itemFactorsRDD.collect())

    val userFactorsRdd= userFactorsRaw.rdd.map(x=>(x.getLong(0),x.getSeq[Double](1).toArray.map(e => e.toFloat)))

    val userWatchedRdd = userWatchedDF.rdd
      .map(r => (r.getLong(0), r.getString(1).toInt))
      .groupByKey()
      .map(e => (e._1, e._2.toArray))

    val userExposedRdd=userExposedDF.rdd
      .map(r => (r.getLong(0), r.getString(1).toInt))
      .groupByKey()
      .map(e => (e._1, e._2.toArray))

    val recommends=userFactorsRdd.leftOuterJoin(userWatchedRdd)
      .leftOuterJoin(userExposedRdd).repartition(500)
      .mapPartitions(partition=>{
        lazy val order = new OrderByScore
        val result = partition.map(x=>{
          val uid = x._1

          val userWatched = x._2._1._2 match {
            case Some(y) => y.toSet
            case _ => Set.empty[Int]
          }

          val exposures2user = x._2._2 match {
            case Some(y) => y.toSet
            case _ => Set.empty[Int]
          }

          val available = bcItemVectors.value.filter(y => {
            val sid = y._1
            !userWatched.contains(sid) && !exposures2user.contains(sid)
          })

          val likeV = available.
            map(y=> (y._1,VectorFunctions.denseProduct(y._2,x._2._1._1)))
          (uid,TopN.apply(topN,likeV)(order).toArray)
        })

        result.map(e => (e._1, e._2.take(topN)))
      })

    recommends.flatMap(e => e._2.map(x => (e._1, x._1, x._2.toDouble)))
      .toDF("uid", "sid", "score")
  }

  override def execute(): Unit = {
    try {
      //----读取HDFS数据-----
      //1.获取用户看过的节目
      val numDaysOfWatched = 300
      val startDate = DateUtils.farthestDayWithDelimiter(numDaysOfWatched)
      val endDate = DateUtils.todayWithDelimiter
      val userWatchedPath: HdfsPath = new HdfsPath(PathConstants.pathOfMoretvLongVideoHistory,
        s"select userid, sid_or_subject_code from tmp where latest_optime >= '$startDate' and latest_optime <= '$endDate'")
      val userWatched = DataReader.read(userWatchedPath).withColumnRenamed("userid","uid").withColumnRenamed("sid_or_subject_code","sid")
      BizUtils.getDataFrameInfo(userWatched,"userWatched")

      //2.获取首页曝光给用户的长视频
      val frontPageExposedLongVideos = BizUtils.getDataFrameWithDataRange(PathConstants.pathOfMoretvFrontPageExposureLongVideos,90).select("userid","sid")
        .withColumnRenamed("userid","uid")
      BizUtils.getDataFrameInfo(frontPageExposedLongVideos,"frontPageExposedLongVideos")

      //3.读取moretv长视频有效节目
      val videoData=BizUtils.getValidLongVideoSid
      BizUtils.getDataFrameInfo(videoData,"videoData")

      //4.获取“编辑精选”标签的电影
      val weightVideos = BizUtils.sidFromEditor(PathConstants.weightOfSelectMovies)
      println("weightVideos.size:"+weightVideos.size)

      //load ALSmodel
      val alsModel = new AlsModel()
      val alsModelPath = new HdfsPath(PathConstants.tmp_als_model_dir)
//      alsModel.loadModel(alsModelPath)
      val userFactorsRaw = alsModel.matrixU
      val itemFactorRaw = alsModel.matrixV
      BizUtils.getDataFrameInfo(userFactorsRaw,"userFactorsRaw")
      BizUtils.getDataFrameInfo(itemFactorRaw,"itemFactorRaw")

      //首页今日推荐的基础数据
      val alsResult = ALSRecommend(userFactorsRaw, itemFactorRaw, 60,
        videoData, weightVideos, userWatched, frontPageExposedLongVideos)
      new DataWriter2Hdfs().write(alsResult, new HdfsPath(PathConstants.tmp_als_recommend_dir))

      //als vip recommend 首页会员看看的基础数据
      val vipVideoData=BizUtils.getVipSid(-1)
      val alsVipResult = ALSRecommend(userFactorsRaw, itemFactorRaw, 100,
        vipVideoData, weightVideos, userWatched, frontPageExposedLongVideos)
      new DataWriter2Hdfs().write(alsVipResult, new HdfsPath(PathConstants.tmp_als_vip_recommend_dir))
    } catch {
      case e: Exception => throw e
    }
  }

}

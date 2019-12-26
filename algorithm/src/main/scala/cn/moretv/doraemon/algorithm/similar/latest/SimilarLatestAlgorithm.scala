package cn.moretv.doraemon.algorithm.similar.latest

import cn.moretv.doraemon.common.alg.Algorithm
import cn.moretv.doraemon.common.util.{ArrayUtils, DateUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
  * 用于给用户推荐看过的最后一部电影的相似内容
  *
  * @author wang.baozhi
  * @since 2018/7/30 上午11:17 
  */
class SimilarLatestAlgorithm extends Algorithm{
  override protected val algParameters: SimilarLatestParameter = new SimilarLatestParameter()
  override protected val modelOutput: SimilarLatestModel = new SimilarLatestModel()

  //数据Map的Key定义
  //用户观看的最后一部电影
  val INPUT_USER_WATCHED_LAST_MOVIES = "userWatchedLastMovies"
  //相似影片数据
  val INPUT_SIMILAR_MOVIE = "similarMovie"
  //用户近期看过的电影
  val INPUT_USER_WATCHED_MOVIES = "userWatchedMovies"
  //首页曝光给用户的长视频
  val INPUT_FRONT_PAGE_EXPOSED_LONG_VIDEOS = "frontPageExposedLongVideos"

  override protected def beforeInvoke(): Unit = {
    dataInput.get(INPUT_USER_WATCHED_LAST_MOVIES) match {
      case None => throw new IllegalArgumentException("未设置输入数据:用户观看的最后一部电影")
      case _ =>
    }
    dataInput.get(INPUT_SIMILAR_MOVIE) match {
      case None => throw new IllegalArgumentException("未设置输入数据:相似影片数据")
      case _ =>
    }
    dataInput.get(INPUT_USER_WATCHED_MOVIES) match {
      case None => throw new IllegalArgumentException("未设置输入数据:用户近期看过的电影")
      case _ =>
    }
    dataInput.get(INPUT_FRONT_PAGE_EXPOSED_LONG_VIDEOS) match {
      case None => throw new IllegalArgumentException("未设置输入数据:首页曝光给用户的长视频")
      case _ =>
    }
  }

  override protected def invoke(): Unit = {
    val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    import ss.implicits._
    /**
      * ---------------------------------------------获取数据，并转换---------------------------------------------
      */
    //1.获取用户观看的最后一部电影 RDD[uid, (sid, score)]
    val endDateImpression = DateUtils.todayWithDelimiter
    val startDateImpression = DateUtils.farthestDayWithDelimiter(algParameters.numOfDaysRetainAnImpression)
    val userWatchedLastMovies=dataInput(INPUT_USER_WATCHED_LAST_MOVIES)
      .filter(s"userid is not null and latest_optime >= '$startDateImpression' and latest_optime <= '$endDateImpression' and score > ${algParameters.thresholdScoreOfUserPrefer}")
      .rdd.map(r => (r.getLong(0), (r.getString(1), r.getString(2), r.getDouble(3)))).
      groupByKey().map(e => (e._1, e._2.toArray.sortBy(x => x._1).last)).map(e => (e._1, (e._2._2, e._2._3)))

    //2.获取相似影片数据 Map(sid,Array[sid])
    val similarMoviesMap=dataInput(INPUT_SIMILAR_MOVIE).rdd.map(r => (r.getString(0), (r.getString(1), r.getDouble(2))))
      .groupByKey().map(e => (e._1, e._2.toArray.sortBy(-_._2).map(x => x._1).take(10))).collectAsMap()

    //3.获取用户近期看过的电影 RDD[(uid, Array[sid])]
    val endDateWatched = DateUtils.todayWithDelimiter
    val startDateWatched = DateUtils.farthestDayWithDelimiter(algParameters.numOfDaysUserWatchedLongVideos)
    val userWatchedMovies=dataInput(INPUT_USER_WATCHED_MOVIES)
      .filter(s"userid is not null and latest_optime >= '$startDateWatched' and latest_optime <= '$endDateWatched'")
      .rdd.map(r => (r.getLong(0), r.getString(1)))
      .groupByKey().map(e => (e._1, e._2.toArray))

    //4.获取首页曝光给用户的长视频 RDD[(uid, Set[sid])]
    val frontPageExposedLongVideos=dataInput(INPUT_FRONT_PAGE_EXPOSED_LONG_VIDEOS).rdd
      .map(r => (r.getLong(0), r.getString(1)))
      .groupByKey()
      .map(r => (r._1, r._2.toArray.distinct.toSet))

    /**
      *------------------------------------计算---------------------------------------------
      * */
    //1.获得某个sid的相似电影 RDD[(uid, Array[sid]],array size is 5
    val similarityRecommend = userWatchedLastMovies.map(e => (e._1, e._2._1)).map(e => {
      val uid = e._1
      val emptyArray = new Array[String](0)
      val similarMovies = similarMoviesMap.getOrElse(e._2, emptyArray)
      (uid, similarMovies)
    }).filter(e => e._2.length > 0)


    //---------2.过滤用户看过的电影与首页曝光给用户的长视频---------
    //筛除掉首页曝光给用户的长视频，RDD[(uid, Array[sid]]
    val filteredSimilarityRecommend = similarityRecommend
      .leftOuterJoin(frontPageExposedLongVideos)
      .map(e => {
        val uid = e._1
        val similarityResult = e._2._1
        val exposures2user = e._2._2 match {
          case Some(x) => x
          case _ => Set.empty[String]
        }

        val filterRecommend = similarityResult.filter(x => !exposures2user.contains(x))

        (uid, filterRecommend)
      }).persist(StorageLevel.DISK_ONLY)

    //进一步筛除掉用户看过的长视频，RDD[(uid, Array[sid]]
    val recommend = filteredSimilarityRecommend
      .leftOuterJoin(userWatchedMovies)
      .map(e => {
        val uid = e._1
        val similarityResult = e._2._1
        val userWatched = e._2._2 match {
          case Some(x) => x
          case _ => Array.empty[String]
        }

        val filterRecommend = similarityResult.filter(x => !userWatched.contains(x))

        (uid, filterRecommend)
      }).persist(StorageLevel.DISK_ONLY)

    //
    modelOutput.similarLatestModel = recommend
      .flatMap(e => e._2.map(x => (e._1, x))).toDF(modelOutput.uidColName, modelOutput.sidColName)
  }

  override protected def afterInvoke(): Unit = {

  }


}

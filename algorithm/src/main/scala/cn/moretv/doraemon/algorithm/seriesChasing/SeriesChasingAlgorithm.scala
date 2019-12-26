package cn.moretv.doraemon.algorithm.seriesChasing

import cn.moretv.doraemon.common.alg.Algorithm
import cn.moretv.doraemon.common.util.DateUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.ArrayBuffer

/**
  * Created by cheng_huan on 2018/6/26.
  */
class SeriesChasingAlgorithm extends Algorithm{
  //数据Map的Key定义
  val INPUT_SCOREDATA_KEY = "scoreData"
  val INPUT_VIDEODATA_KEY = "videoData"

  //输入输出属性定义
  val algParameters: SeriesChasingParameters = new SeriesChasingParameters()
  val modelOutput: SeriesChasingModel = new SeriesChasingModel()

  override def beforeInvoke(): Unit = {
    /*dataInput.get(INPUT_SCOREDATA_KEY) match {
      case None => throw new IllegalArgumentException("未设置输入数据")
    }
    dataInput.get(INPUT_VIDEODATA_KEY) match {
      case None => throw new IllegalArgumentException("未设置输入数据")
    }*/
  }

  override def invoke(): Unit = {
    /**
      * userDataRaw: DataFrame[(uid, sid, latest_optime, eposideIndex)]
      * latestActive: DataFrame[(uid, latestActive_time)]
      * videoData: DataFrame[(sid, Array[(episodeIndex, createTime)])]
      * paramArray: Array[(contentType, thresholdWatchedStoryNum, thresholdScore, numOfDays)]
      * path4Recommendation: String
      */
    val scoreData = dataInput(INPUT_SCOREDATA_KEY)
    val videoData = dataInput(INPUT_VIDEODATA_KEY)
    val paramArray = algParameters.paramArray

    seriesChasingRecommend4AllLongVideos(scoreData,videoData, paramArray)
  }

  override def afterInvoke(): Unit = {

  }

  /**
    * 对用户原始数据进行整合
    * @param rawData rawData DataFrame[(uid, sid, latest_optime, episodeIndex)]
    * @param latestActive DataFrame[(uid, latestActive_time)]
    * @return RDD[(uid, latestActive_time, Array[(sid, latest_opTime, latest_episodeIndex, penultimate_episodeIndex)])]
    */
  def userBehaviorIntegrate(rawData: DataFrame, latestActive: DataFrame):
  RDD[(Long, String, Array[(String, String, Int, Int)])] = {
    val result = rawData.rdd.map(e => ((e.getLong(0), e.getString(1)), (e.getString(2), e.getInt(3))))
      .groupByKey()
      .map(e => (e._1, e._2.toArray.sortBy(x => x._1).takeRight(2)))
      .filter(e => e._2.nonEmpty)
      .map(e => {
        val uid = e._1._1
        val sid = e._1._2
        val watches = e._2

        var latest_opTime = ""
        var latest_episodeIndex = -1
        var penultimate_episodeIndex = 0

        if(watches.length == 2) {
          latest_opTime = watches(1)._1
          latest_episodeIndex = watches(1)._2
          penultimate_episodeIndex = watches(0)._2
        } else {
          latest_opTime = watches(0)._1
          latest_episodeIndex = watches(0)._2
        }

        (uid, (sid, latest_opTime, latest_episodeIndex, penultimate_episodeIndex))
      })
      .groupByKey()
      .map(e => (e._1, e._2.toArray))

    result.join(latestActive.rdd.map(e => (e.getLong(0), e.getString(1))))
      .map(e => (e._1, e._2._2, e._2._1))
  }

  /**
    * 对用户原始数据进行整合
    * @param rawData DataFrame[(uid, sid, latest_optime, episodeIndex)]
    * @param latestActive DataFrame[(uid, latestActive_time)]
    * @return RDD[(uid, latestActive_time, Array[(sid, latest_opTime, numOfLatestDayWatchedEpisodes,
    *         numOfDaysContinuouslyWatch, maxWatchedEpisodeIndex])])]
    */
  def userBehaviorIntegrate4IndependentShortStory(rawData: DataFrame, latestActive: DataFrame):
  RDD[(Long, String, Array[(String, String, Int, Int, Int)])] = {
    val result = rawData.rdd.map(e => ((e.getLong(0), e.getString(1)), (e.getString(2), e.getInt(3))))
      .groupByKey()
      .filter(e => e._2.nonEmpty)
      .map(e => {
        val uid = e._1._1
        val sid = e._1._2
        val watchHistory = e._2.map(x => (x._1.substring(0, 10), x._2)).toArray
        val latest_opTime = watchHistory.map(x => x._1).max
        val numOfLatestDayWatchedEpisodes = watchHistory.filter(x => x._1 == latest_opTime).length
        val maxWatchedEpisodeIndex = watchHistory.map(x => x._2).max
        val dateArray = watchHistory.map(x => x._1).distinct
        var numOfDaysContinuouslyWatch = 0
        var continuous = true
        while (continuous && numOfDaysContinuouslyWatch < dateArray.length) {
          numOfDaysContinuouslyWatch += 1
          val day = DateUtils.farthestDay2referenceWithDelimiter(latest_opTime, -numOfDaysContinuouslyWatch)
          if(!dateArray.contains(day)){
            continuous = false
          }
        }

        (uid, (sid, latest_opTime, numOfLatestDayWatchedEpisodes, numOfDaysContinuouslyWatch, maxWatchedEpisodeIndex))
      })
      .groupByKey()
      .map(e => (e._1, e._2.toArray))

    result.join(latestActive.rdd.map(e => (e.getLong(0), e.getString(1))))
      .map(e => (e._1, e._2._2, e._2._1))
  }

  /**
    * 对用户原始数据进行整合
    * @param rawData DataFrame[(uid, sid, latest_optime, episodeIndex)]
    * @param latestActive DataFrame[(uid, latestActive_time)]
    * @return RDD[(uid, latestActive_time, Array[(sid, latest_opTime, numOfLatestDayWatchedEpisodes, numOfWatchedEpisodes, maxWatchedEpisodeIndex])])]
    */
  def userBehaviorIntegrate4IndependentLongStory(rawData: DataFrame, latestActive: DataFrame):
  RDD[(Long, String, Array[(String, String, Int, Int, Int)])] = {
    val result = rawData.rdd.map(e => ((e.getLong(0), e.getString(1)), (e.getString(2), e.getInt(3))))
      .groupByKey()
      .filter(e => e._2.nonEmpty)
      .map(e => {
        val uid = e._1._1
        val sid = e._1._2
        val watchHistory = e._2.map(x => (x._1.substring(0, 10), x._2)).toArray
        val latest_opTime = watchHistory.map(x => x._1).max
        val numOfLatestDayWatchedEpisodes = watchHistory.filter(x => x._1 == latest_opTime).length
        val numOfWatchedEpisodes = watchHistory.length
        val maxWatchedEpisodeIndex = watchHistory.map(x => x._2).max

        (uid, (sid, latest_opTime, numOfLatestDayWatchedEpisodes, numOfWatchedEpisodes, maxWatchedEpisodeIndex))
      })
      .groupByKey()
      .map(e => (e._1, e._2.toArray))

    result.join(latestActive.rdd.map(e => (e.getLong(0), e.getString(1))))
      .map(e => (e._1, e._2._2, e._2._1))
  }

  /**
    * 将用户行为与视频信息进行整合
    * @param userBehavior RDD[(uid, latestActive_time,
    *                     Array[(sid, lastest_optime, latest_episodeIndex, penultimate_episodeIndex)])]
    * @param videoData DataFrame[(sid, eposideIndex, updateTime)]
    * @return RDD[(uid, latestActive_time, Array[(sid, maxValidEpisodeIndex,
    *         maxValidEpisodeIndex_latestActive, lastest_optime, latest_episodeIndex, penultimate_episodeIndex)])]
    */
  def userBehaviorAndVideoDataIntegrate(userBehavior: RDD[(Long, String, Array[(String, String, Int, Int)])],
                                        videoData: DataFrame):
  RDD[(Long, String, Array[(String, Int, Int, String, Int, Int)])] = {
    val videoDataMap = videoData.rdd.map(e => (e.getString(0), (e.getInt(1), e.getString(2))))
      .groupByKey().map(e => (e._1, e._2.toArray.sortBy(x => -x._1))).collectAsMap() //按集数逆序排列

    println(s"videoData = ${videoData.count()}")
    videoData.printSchema()
    println(s"videoDataMap = ${videoDataMap.size}")

    userBehavior.map(e => {
      val uid = e._1
      val latestActive_time = e._2
      val result = e._3.map(x => {
        val sid = x._1
        val placeHolderArray = Array((-2, ""))
        val videoData = videoDataMap.getOrElse(sid, placeHolderArray)
        val videoDataBeforeLatestActive = videoData.filter(y => y._2 <= latestActive_time)

        val maxValidEpisodeIndex = videoData.head._1
        val maxValidEpisodeIndex_latestActive = videoDataBeforeLatestActive.length match {
          case 0 => -1
          case _ => videoDataBeforeLatestActive.head._1
        }

        (sid, maxValidEpisodeIndex, maxValidEpisodeIndex_latestActive, x._2, x._3, x._4)
      })

      (uid, latestActive_time, result)
    })
      .map(e => (e._1, e._2, e._3.filter(x => x._2 != -2)))
      .filter(e => e._3.length > 0)
  }

  /**
    *  将用户行为与视频更新信息整合（独立剧情 + 短故事）
    * @param userBehavior RDD[(uid, latestActive_time, Array[(sid, latest_opTime, numOfLatestDayWatchedEpisodes,
    *         numOfDaysContinuouslyWatch, maxWatchedEpisodeIndex])])]
    * @param videoData DataFrame[(sid, eposideIndex, updateTime)]
    * @return RDD[(uid, latestActive_time, Array[(sid, maxValidEpisodeIndex, maxValidEpisodeIndex_latestActive,
    *         latest_opTime, numOfLatestDayWatchedEpisodes, numOfDaysContinuouslyWatch, maxWatchedEpisodeIndex)])]
    */
  def userBehaviorAndVideoDataIntegrate4IndependentShortStory(userBehavior: RDD[(Long, String, Array[(String, String, Int, Int, Int)])],
                                                              videoData: DataFrame):
  RDD[(Long, String, Array[(String, Int, Int, String, Int, Int, Int)])] = {
    val videoDataMap = videoData.rdd.map(e => (e.getString(0), (e.getInt(1), e.getString(2))))
      .groupByKey().map(e => (e._1, e._2.toArray.sortBy(x => -x._1))).collectAsMap() //按集数逆序排列

    userBehavior.map(e => {
      val uid = e._1
      val latestActive_time = e._2
      val result = e._3.map(x => {
        val sid = x._1
        val placeHolderArray = Array((-2, ""))
        val videoData = videoDataMap.getOrElse(sid, placeHolderArray)
        val videoDataBeforeLatestActive = videoData.filter(y => y._2 <= latestActive_time)

        val maxValidEpisodeIndex = videoData.head._1
        val maxValidEpisodeIndex_latestActive = videoDataBeforeLatestActive.length match {
          case 0 => -1
          case _ => videoDataBeforeLatestActive.head._1
        }

        (sid, maxValidEpisodeIndex, maxValidEpisodeIndex_latestActive, x._2, x._3, x._4, x._5)
      })

      (uid, latestActive_time, result)
    })
      .map(e => (e._1, e._2, e._3.filter(x => x._2 != -2)))
      .filter(e => e._3.length > 0)
  }

  /**
    * 将用户行为与视频更新信息整合（独立剧情 + 长故事）
    * @param numOfDays 取的日志天数
    * @param userBehavior RDD[(uid, latestActive_time, Array[(sid, latest_opTime,
    *                     numOfLatestDayWatchedEpisodes, numOfWatchedEpisodes, maxWatchedEpisodeIndex])])]
    * @param videoData DataFrame[(sid, eposideIndex, updateTime)]
    * @return RDD[(uid, latestActive_time, Array[(sid, maxValidEpisodeIndex, maxValidEpisodeIndex_latestActive, latest_opTime,
    *         numOfLatestDayWatchedEpisodes, numOfWatchedEpisodes, numOfUpdateEpisodes, maxWatchedEpisodeIndex)])]
    */
  def userBehaviorAndVideoDataIntegrate4IndependentLongStory(numOfDays: Int,
                                                             userBehavior: RDD[(Long, String, Array[(String, String, Int, Int, Int)])],
                                                             videoData: DataFrame):
  RDD[(Long, String, Array[(String, Int, Int, String, Int, Int, Int, Int)])] = {
    val startDate = DateUtils.farthestDayWithOutDelimiter(numOfDays)
    val videoDataRDD = videoData.rdd.map(e => (e.getString(0), (e.getInt(1), e.getString(2))))
      .groupByKey().map(e => (e._1, e._2.toArray))

    // (sid, ((latest_opTime, numOfLatestDayWatchedEpisodes, numOfWatchedEpisodes, maxWatchedEpisodeIndex), (uid, latestActive_time)), Array(eposideIndex, updateTime))
    userBehavior.flatMap(e => e._3.map(x => (x._1, ((x._2, x._3, x._4, x._5), (e._1, e._2)))))
      .leftOuterJoin(videoDataRDD)
      .map(e => {
        val uid = e._2._1._2._1
        val latestActive_time = e._2._1._2._2

        val sid = e._1
        val latest_opTime = e._2._1._1._1
        val numOfLatestDayWatchedEpisodes = e._2._1._1._2
        val numOfWatchedEpisodes = e._2._1._1._3
        val maxWatchedEpisodeIndex = e._2._1._1._4

        val videoDataArray = e._2._2 match {
          case Some(x) => x
          case _ => Array.empty[(Int, String)]
        }

        ((uid, latestActive_time), (sid, latest_opTime, numOfLatestDayWatchedEpisodes, numOfWatchedEpisodes, maxWatchedEpisodeIndex, videoDataArray))
      })
      .groupByKey()
      .map(e => {
        val uid = e._1._1
        val latestActive_time = e._1._2
        //sid, latest_opTime, numOfLatestDayWatchedEpisodes, numOfWatchedEpisodes, maxWatchedEpisodeIndex, videoDataArray
        val result = e._2.toArray.map(x => {
          val sid = x._1
          val videoData = x._6
          val videoDataBeforeLatestActive = videoData.filter(y => y._2 <= latestActive_time)

          val maxValidEpisodeIndex = videoData.length match {
            case 0 => -2
            case _ => videoData.maxBy(x => x._1)._1
          }

          val maxValidEpisodeIndex_latestActive = videoDataBeforeLatestActive.length match {
            case 0 => -1
            case _ => videoDataBeforeLatestActive.maxBy(x => x._1)._1
          }

          val numOfUpdateEpisodes = x._6.filter(y => y._2 >= startDate).length

          (sid, maxValidEpisodeIndex, maxValidEpisodeIndex_latestActive, x._2, x._3, x._4, numOfUpdateEpisodes, x._5)
          // (sid, maxValidEpisodeIndex, maxValidEpisodeIndex_latestActive, latest_opTime, numOfLatestDayWatchedEpisodes, numOfWatchedEpisodes, numOfUpdateEpisodes, maxWatchedEpisodeIndex)
        })

        (uid, latestActive_time, result)
      })
      .map(e => (e._1, e._2, e._3.filter(x => x._2 != -2)))
      .filter(e => e._3.length > 0)
  }

  /**
    * 判断用户是否在追剧，并过滤
    * 判断条件：1、正向观看（非随机观看）
    *           2、有内容
    *           3、未弃剧（弃剧条件：当时有内容 + 没空 + 看了别的）
    * @param userData RDD[(uid, latestActive_time,
    *         Array[(sid, maxValidEpisodeIndex, maxValidEpisodeIndex_latestActive, lastest_optime, latest_episodeIndex, penultimate_episodeIndex)])]
    * @return RDD[(uid, Array[sid])]
    */
  def seriesChasingFilter(thresholdNumOfSeriesWatched: Int, //此参数为了维护函数接口的一致性，实际不用
                          userData: RDD[(Long, String, Array[(String, Int, Int, String, Int, Int)])]):
  RDD[(Long, Array[String])] = {
    userData.map(e => {
      val uid = e._1
      val latestActive_time = e._2
      val result = e._3.filter(x => {
        val maxValidEpisodeIndex = x._2
        val maxValidEpisodeIndex_latestActive = x._3
        val latest_optime = x._4
        val latest_episodeIndex = x._5
        val penultimate_episodeIndex = x._6

        (latest_episodeIndex > penultimate_episodeIndex) && //正向观看（非随机观看）
          (maxValidEpisodeIndex > latest_episodeIndex) && //有内容
          !(maxValidEpisodeIndex_latestActive > latest_episodeIndex //弃剧（当时有内容 + 没空 + 看了别的:日期截止到天）
            && latestActive_time.substring(0,10) > latest_optime.substring(0,10))
      }).map(y => y._1)

      (uid, result)
    })
      .filter(e => e._2.length > 0)

  }

  /**
    * 判断用户是否在追剧（独立剧情），并过滤
    * 判断条件：1、最后活跃日看了N1集（参数可调）or 最后活跃日已连续观看了N2天（参数可调）
    *           2、有内容
    *           3、未弃剧（弃剧条件：当时有内容 + 没空 + 看了别的 || 曝光 + 未看）
    * @param thresholdNumOfIndependentShortStoriesWatched 观看集数的阈值
    * @param userData userData RDD[(uid, latestActive_time, Array[(sid, maxValidEpisodeIndex, maxValidEpisodeIndex_latestActive,
    *         latest_opTime, numOfLatestDayWatchedEpisodes, numOfDaysContinuouslyWatch, maxWatchedEpisodeIndex)])]
    * @return RDD[(uid, Array[sid])]
    */
  def seriesChasingFilter4IndependentShortStory(thresholdNumOfIndependentShortStoriesWatched: Int,
                                                userData: RDD[(Long, String, Array[(String, Int, Int, String, Int, Int, Int)])]):
  RDD[(Long, Array[String])] = {
    userData.map(e => {
      val uid = e._1
      val latestActive_time = e._2
      val result = e._3.filter(x => {
        val maxValidEpisodeIndex = x._2
        val maxValidEpisodeIndex_latestActive = x._3
        val latest_optime = x._4
        val numOfLatestDayWatchedEpisodes = x._5
        val numOfDaysContinuouslyWatch = x._6
        val maxWatchedEpisodeIndex = x._7

        (numOfLatestDayWatchedEpisodes > thresholdNumOfIndependentShortStoriesWatched
          || numOfDaysContinuouslyWatch > algParameters.thresholdNumOfDaysContinuouslyWatched) &&
          (maxValidEpisodeIndex > maxWatchedEpisodeIndex) &&
          !(maxValidEpisodeIndex_latestActive > maxWatchedEpisodeIndex //弃剧（当时有内容 + 没看 + 看了别的:日期截止到天）
            && latestActive_time.substring(0,10) > latest_optime.substring(0,10))
      }).sortBy(y => (-y._5, -y._6)).map(y => y._1)

      (uid, result)
    })
      .filter(e => e._2.length > 0)

  }

  /**
    * 判断用户是否在追剧（独立剧情 + 长故事），并过滤
    * 判断条件：1、（最后一天观看集数 >= threshold）||（剧集更新集数 > 0 && 观看集数 / 剧集更新集数 >= threshold(默认：1)）
    *           2、有内容
    *           3、未弃剧（弃剧条件：当时有内容 + 没空 + 看了别的 || 曝光 + 未看）
    * @param thresholdNumOfIndependentLongStoriesWatched 观看集数的阈值
    * @param userData RDD[(uid, latestActive_time, Array[(sid, maxValidEpisodeIndex, maxValidEpisodeIndex_latestActive,
    *         latest_opTime, numOfLatestDayWatchedEpisodes, numOfWatchedEpisodes, numOfUpdateEpisodes, maxWatchedEpisodeIndex)])]
    * @return RDD[(uid, Array)]
    */
  def seriesChasingFilter4IndependentLongStory(thresholdNumOfIndependentLongStoriesWatched: Int,
                                               userData: RDD[(Long, String, Array[(String, Int, Int, String, Int, Int, Int, Int)])]):
  RDD[(Long, Array[String])] = {
    userData.map(e => {
      val uid = e._1
      val latestActive_time = e._2
      val result = e._3.filter(x => {
        val maxValidEpisodeIndex = x._2
        val maxValidEpisodeIndex_latestActive = x._3
        val latest_optime = x._4
        val numOfLatestDayWatchedEpisodes = x._5
        val numOfWatchedEpisodes = x._6
        val numOfUpdateEpisodes = x._7
        val maxWatchedEpisodeIndex = x._8

        (numOfLatestDayWatchedEpisodes >= thresholdNumOfIndependentLongStoriesWatched || (numOfUpdateEpisodes > 0 &&
          numOfWatchedEpisodes.toDouble / numOfUpdateEpisodes >= algParameters.thresholdOfWatch2UpdateRatio)) &&
          (maxValidEpisodeIndex > maxWatchedEpisodeIndex) && //有内容
          !(maxValidEpisodeIndex_latestActive > maxWatchedEpisodeIndex //弃剧（当时有内容 + 没看 + 看了别的:日期截止到天）
            && latestActive_time.substring(0,10) > latest_optime.substring(0,10))
      }).sortBy(y => -y._5).map(y => y._1)

      (uid, result)
    })
      .filter(e => e._2.length > 0)

  }

  /**
    * 追剧模型推荐
    * @param contentType 视频类型
    * @return RDD[(uid, Array[sid])]
    */
  def seriesChasingRecommend(userDataRaw: DataFrame,
                             latestActive: DataFrame,
                             videoData: DataFrame,
                             contentType: String,
                             thresholdWatchedStoryNum: Int) = {
    val userBehavior = userBehaviorIntegrate(userDataRaw, latestActive)
    val userBehaviorAndVideoData = userBehaviorAndVideoDataIntegrate(userBehavior, videoData)

    println(s"userBehavior = ${userBehavior.count()}")
    println(s"userBehaviorAndVideoData = ${userBehaviorAndVideoData.count()}")

    userBehavior.take(10)
      .foreach(e => {
        println(s"uid = ${e._1}")
        e._3.foreach(x => println(x.toString()))
      })
    //RDD[(uid, latestActive_time, Array[(sid, maxValidEpisodeIndex,
    //        maxValidEpisodeIndex_latestActive, lastest_optime, latest_episodeIndex, penultimate_episodeIndex)])]

    userBehaviorAndVideoData.filter(e => e._1 == 33963907L).collect()
      .foreach(e => {
        //(sid, maxValidEpisodeIndex, maxValidEpisodeIndex_latestActive, lastest_optime, latest_episodeIndex, penultimate_episodeIndex)
        val latestActive_time = e._2
        e._3.foreach(x => {
          val maxValidEpisodeIndex = x._2
          val maxValidEpisodeIndex_latestActive = x._3
          val latest_optime = x._4
          val latest_episodeIndex = x._5
          val penultimate_episodeIndex = x._6

          println("=======================")
          println(s"sid = ${x._1}")

          println(s"正向观看（非随机观看）：${(latest_episodeIndex > penultimate_episodeIndex)}")
          println(s"latest_episodeIndex = $latest_episodeIndex")
          println(s"penultimate_episodeIndex = $penultimate_episodeIndex")
          println()
          println(s"有内容条件：${maxValidEpisodeIndex > latest_episodeIndex}")
          println(s"maxValidEpisodeIndex = $maxValidEpisodeIndex")
          println(s"maxWatchedEpisodeIndex = $latest_episodeIndex")
          println()
          println(s"弃剧条件：${!(maxValidEpisodeIndex_latestActive > latest_episodeIndex
            && latestActive_time.substring(0,10) > latest_optime.substring(0,10))}")
          println(s"maxValidEpisodeIndex_latestActive = $maxValidEpisodeIndex_latestActive")
          println()
        })})

    seriesChasingFilter(thresholdWatchedStoryNum, userBehaviorAndVideoData)
  }

  /**
    * 追剧模型推荐（独立剧情）
    * @param contentType 视频类型
    * @return RDD[(uid, Array[sid])]
    */
  def seriesChasingRecommend4IndependentShortStory(userDataRaw: DataFrame,
                                                   latestActive: DataFrame,
                                                   videoData: DataFrame,
                                                   contentType: String,
                                                   thresholdWatchedStoryNum: Int):RDD[(Long, Array[String])] = {
    val userBehavior = userBehaviorIntegrate4IndependentShortStory(userDataRaw, latestActive)
    val userBehaviorAndVideoData = userBehaviorAndVideoDataIntegrate4IndependentShortStory(userBehavior, videoData)
    seriesChasingFilter4IndependentShortStory(thresholdWatchedStoryNum, userBehaviorAndVideoData)
  }

  /**
    * 追剧模型推荐（独立剧情 + 长故事）
    * @param contentType 视频类型
    * @return RDD[(uid, Array[sid])]
    */
  def seriesChasingRecommend4IndependentLongStory(userDataRaw: DataFrame,
                                                  latestActive: DataFrame,
                                                  videoData: DataFrame,
                                                  contentType: String,
                                                  thresholdWatchedStoryNum: Int,
                                                  numOfDays: Int):RDD[(Long, Array[String])] = {
    val userBehavior = userBehaviorIntegrate4IndependentLongStory(userDataRaw, latestActive)
    val userBehaviorAndVideoData = userBehaviorAndVideoDataIntegrate4IndependentLongStory(numOfDays, userBehavior, videoData)
    seriesChasingFilter4IndependentLongStory(thresholdWatchedStoryNum, userBehaviorAndVideoData)
  }

  /**
    * 适用于所有长视频的追剧模型
    * @param paramArray 参数列表 Array[(contentType, thresholdScore, numOfDays)]
    */
  def seriesChasingRecommend4AllLongVideos(seriesScoreData: DataFrame,
                                           videoDataAll: DataFrame,
                                           paramArray:Array[(String, Int, Double, Int)]) = {
    val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    val sc: SparkContext = ss.sparkContext
    import ss.implicits._
    var union = sc.makeRDD(new Array[(Long, Array[String])](0))
    var part = sc.makeRDD(new Array[(Long, Array[String])](0))

    paramArray.foreach(e => {
      val contentType = e._1
      val thresholdWatchedStoryNum = e._2
      val thresholdScore = e._3
      val numOfDays = e._4

      val userDataRaw = scoreDataFilter2userDataRaw(seriesScoreData, contentType, thresholdScore)
      val latestActive = scoreDataFilter2UserLatestActive(seriesScoreData, thresholdScore)
      val videoData = videoDataFilter2featuresOnSeriesChasing(videoDataAll, contentType)

      if(contentType == "tv") {
        part = seriesChasingRecommend(userDataRaw, latestActive, videoData, contentType, thresholdWatchedStoryNum)
          .persist(StorageLevel.MEMORY_AND_DISK)
      } else if(contentType == "kids") {
        part = seriesChasingRecommend4IndependentShortStory(userDataRaw, latestActive, videoData, contentType, thresholdWatchedStoryNum)
          .persist(StorageLevel.MEMORY_AND_DISK)
      } else {
        part = seriesChasingRecommend4IndependentLongStory(userDataRaw, latestActive, videoData, contentType, thresholdWatchedStoryNum, numOfDays)
          .persist(StorageLevel.MEMORY_AND_DISK)
      }

      println(s"SeriesChasing4$contentType:")
      val partCount = part.count()
      println(partCount)

      if(partCount > 0) {
        union = union.fullOuterJoin(part).map(e => {
          val uid = e._1
          val union = e._2._1 match {
            case Some(x) => x
            case _ => Array.empty[String]
          }
          val part = e._2._2 match {
            case Some(x) => x
            case _ => Array.empty[String]
          }

          (uid, arrayAlternate(union, part, 1, 1))
        })
      }
    })

    modelOutput.seriesChasingData = union.flatMap(e => e._2.map(x => (e._1, x))).toDF("uid", "sid")
  }

  /**
    * 将两个数组按一定频率穿插组合起来
    * @param a 数组1
    * @param b 数组2
    * @param f1 取数组1元素的频率
    * @param f2 去数组2元素的频率
    * @return 组合后的数组
    */
  def arrayAlternate(a:Array[String], b:Array[String], f1:Int, f2:Int) : Array[String] = {
    val result = new ArrayBuffer[String]()
    var index_1 = 0
    var index_2 = 0

    if(a.length > 0 && b.length > 0) {
      while(index_1 < a.length || index_2 < b.length)
      {
        for(i <- index_1 until index_1 + f1)
        {
          if(i < a.length) result += a(i)
        }
        index_1 = index_1 + f1


        for(j <- index_2 until index_2 + f2)
        {
          if(j < b.length) result += b(j)
        }
        index_2 = index_2 + f2
      }

      result.toArray
    }else{
      if(a.length == 0) {
        b
      }else{
        a
      }
    }
  }

  /**
    * 获取用户在特定周期上观看的长视频
    * @param contentType 类型
    * @param scoreThreshold 评分阈值
    * @return DataFrame[(uid, sid, latest_optime, eposideIndex)]
    */
  def scoreDataFilter2userDataRaw(scoreData: DataFrame,
                                  contentType:String,
                                  scoreThreshold:Double): DataFrame ={

    scoreData.filter("userid is not null and" +
      " sid is not null and " +
      " optime is not null and " +
      s" content_type = '$contentType' and " +
      " episodesid is not null and " +
      " episode_index is not null and " +
      s" score > '$scoreThreshold'").select("userid", "sid", "optime", "episode_index")
  }

  /**
    * 获取用户最后一次活跃的时间
    * @param scoreThreshold 评分阈值
    * @return RDD[(uid, latestActive_time)]
    */
  def scoreDataFilter2UserLatestActive(scoreData: DataFrame,
                                       scoreThreshold:Double): DataFrame ={
    val result = scoreData.filter("userid is not null and" +
      " sid is not null and " +
      " optime is not null and " +
      " content_type is not null and " +
      " episodesid is not null and " +
      " episode_index is not null and " +
      s" score > '$scoreThreshold'").select("userid", "optime").rdd
      .map(r => (r.getLong(0), r.getString(1)))
      .groupByKey()
      .map(e => (e._1, e._2.toArray.sortBy(x => x).reverse.head))

    val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    import ss.implicits._
    result.toDF("userid", "optime")
  }

  /**
    * 获取视频内容信息
    * @return RDD[(parent_sid, Array[(episodeIndex, createTime)])]
    */
  def videoDataFilter2featuresOnSeriesChasing(videoData:DataFrame,
                                             contentType:String): DataFrame = {
    val result = videoData.filter(s"content_type = '$contentType'")
      .select("parent_sid", "episode_index", "create_time").rdd
      .map(r => (r.getString(0), (r.getInt(1), r.getTimestamp(2).toString)))
      .groupByKey()
      .map(e => (e._1, e._2.toArray))
      .flatMap(e => e._2.map(x => (e._1, x._1, x._2)))

    val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    import ss.implicits._
    result.toDF("parent_sid", "episode_index", "create_time")
  }
}

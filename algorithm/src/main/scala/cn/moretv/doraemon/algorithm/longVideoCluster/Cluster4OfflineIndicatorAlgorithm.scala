package cn.moretv.doraemon.algorithm.longVideoCluster

import cn.moretv.doraemon.common.alg.Algorithm
import cn.moretv.doraemon.common.util.{ArrayUtils, DateUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
  *
  * @author wang.baozhi 
  * @since 2018/7/26 下午5:24 
  */
class Cluster4OfflineIndicatorAlgorithm  extends Algorithm{
  override protected val algParameters: Cluster4OfflineIndicatorParameters = new Cluster4OfflineIndicatorParameters()
  override protected val modelOutput: LongVideoClusterModel = new LongVideoClusterModel()

  //数据Map的Key定义
  //用户电影聚类的特征
  val INPUT_USER_FEATURES_BY_LONG_VIDEO_CLUSTER = "userFeaturesByLongVideoCluster"
  //首页曝光给用户的长视频
  val INPUT_FRONT_PAGE_EXPOSED_LONG_VIDEOS = "frontPageExposedLongVideos"
  //长视频聚类信息
  val INPUT_LONG_VIDEO_CLUSTER_DATA = "longVideoClusterData"
  //长视频的有效视频
  val INPUT_VALID_LONG_VIDEO = "validLongVideo"
  //获取用户看过的长视频
  val INPUT_USER_WATCHED_LONG_VIDEOS = "userWatchedLongVideos"

  override protected def beforeInvoke(): Unit = {
    dataInput.get(INPUT_USER_FEATURES_BY_LONG_VIDEO_CLUSTER) match {
      case None => throw new IllegalArgumentException("未设置输入数据:用户电影聚类的特征")
      case _ =>
    }
    dataInput.get(INPUT_FRONT_PAGE_EXPOSED_LONG_VIDEOS) match {
      case None => throw new IllegalArgumentException("未设置输入数据:首页曝光给用户的长视频")
      case _ =>
    }
    dataInput.get(INPUT_LONG_VIDEO_CLUSTER_DATA) match {
      case None => throw new IllegalArgumentException("未设置输入数据:长视频聚类信息")
      case _ =>
    }
    dataInput.get(INPUT_VALID_LONG_VIDEO) match {
      case None => throw new IllegalArgumentException("未设置输入数据:长视频的有效视频")
      case _ =>
    }
    dataInput.get(INPUT_USER_WATCHED_LONG_VIDEOS) match {
      case None => throw new IllegalArgumentException("未设置输入数据:用户看过的长视频")
      case _ =>
    }
  }

  override protected def invoke(): Unit = {
    val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    import ss.implicits._
    /**
      * ---------------------------------------------获取数据，并转换---------------------------------------------
      */
    //读取用户长视频聚类的特征/近期使用的类/长视频聚类信息
    //1.获取用户电影聚类的特征 RDD[(uid, Array[(cluster_Index, maxWatchScore, avgWatchRatio)])]
    val userFeaturesByLongVideoCluster=dataInput(INPUT_USER_FEATURES_BY_LONG_VIDEO_CLUSTER).rdd.
      map(r => (r.getLong(0), (r.getInt(1), r.getDouble(2), r.getDouble(3)))).
      groupByKey().map(e => (e._1, e._2.toArray)).filter(e => e._2.length > 0).
      persist(StorageLevel.DISK_ONLY)

    //2.获取首页曝光给用户的长视频 RDD[(Long, Set[String])]
    val frontPageExposedLongVideos=dataInput(INPUT_FRONT_PAGE_EXPOSED_LONG_VIDEOS).rdd
      .map(r => (r.getLong(0), r.getString(1)))
      .groupByKey()
      .map(r => (r._1, r._2.toArray.distinct.toSet)).persist(StorageLevel.MEMORY_AND_DISK)

    //3.获取长视频聚类信息,RDD[(Int,Array[String])],RDD[(cluster_id,Array[sid])]
    val longVideoClusterData=dataInput(INPUT_LONG_VIDEO_CLUSTER_DATA).rdd.
      map(r => (r.getInt(0), r.getSeq[String](1))).
      map(x => (x._1, x._2.toArray))

    //4.有效视频 RDD[String]
    val validLongVideo=dataInput(INPUT_VALID_LONG_VIDEO).rdd.map(e=>e.getString(0))

    //5.用户看过的长视频 RDD[(Long, Array[String])]
    val endDate = DateUtils.farthestDayWithDelimiter(algParameters.nDayAgoEnd)
    val startDate = DateUtils.farthestDayWithDelimiter(algParameters.numOfDaysUserWatchedLongVideos)
    val userWatchedLongVideos=dataInput(INPUT_USER_WATCHED_LONG_VIDEOS).rdd.
      map(r => (r.getLong(0), (r.getString(1), r.getString(2)))).
      filter(e => e._2._1 >= startDate && e._2._1 < endDate).
      groupByKey().map(e => (e._1, e._2.map(x => x._2).toArray))


    /**
      *------------------------------------计算---------------------------------------------
      * */
    val longVideoInCluster = longVideoClusterData.flatMap(e => e._2)
    val invalidLongVideo = longVideoInCluster.subtract(validLongVideo).collect()

    //过滤无效视频，返回Map[Int,Array[String]]，一个cluster_id对应的sid array
    val cluster2longVideoMap = longVideoClusterData.map(e => (e._1, e._2.filter(x => !invalidLongVideo.contains(x)))).
      filter(e => e._2.size > 0).collectAsMap()

    //RDD[cluster_index,Array[sid]]转变为按array中的sid group by，Map[sid,Array[cluster_index]]
    val longVideo2clusterMap = longVideoClusterData.flatMap(e => e._2.map(x => (x, e._1))).groupByKey()
      .map(e => (e._1, e._2.toArray)).collectAsMap()
    //获得曝光给用户的类别，也就是近期使用的类,RDD[(uid,Array[cluster_id])]
    val exposureLongVideoClusters = frontPageExposedLongVideos.map(e => {
      val uid = e._1
      val exposureClusters = e._2.toArray
        .flatMap(x => longVideo2clusterMap.getOrElse(x, Array.emptyIntArray))
        .groupBy(x => x).toArray.sortBy(x => -x._2.length).map(x => x._1)

      (uid, exposureClusters.take(algParameters.numOfPastBasedLongVideoCluster4Filter))
    })

     //选出用户类别偏好&排序&过滤近期推荐过的类
    //userFeaturesByLongVideoCluster RDD[(uid,Array[(clusterIndex,maxWatchScore,averageWatchRatio)])]
    //exposureLongVideoClusters      RDD[(uid,Array[cluster_id])]
    //输出: RDD[(uid,Array[cluster_id])]
    val processedUserFeatures = userFeaturesByLongVideoCluster.
      leftOuterJoin(exposureLongVideoClusters).map(e => {
      val uid = e._1
      val exposureClusters = e._2._2 match {
        case Some(x) => x
        case _ => new Array[Int](0)
      }

      val selectMovieCluster = e._2._1.filter(x => !exposureClusters.contains(x._1) && x._1 <= 1023)
        .sortBy(x => (-x._2, -x._3))
        .map(x => x._1).take(algParameters.numOfLongVideoCluster4Recommend / 6)
      val selectTvCluster = e._2._1.filter(x => !exposureClusters.contains(x._1) && x._1 >= 1024 && x._1 < 2048)
        .sortBy(x => (-x._2, -x._3))
        .map(x => x._1).take(algParameters.numOfLongVideoCluster4Recommend / 6)
      val selectZongyiCluster = e._2._1.filter(x => !exposureClusters.contains(x._1) && x._1 >= 2048 && x._1 < 2559)
        .sortBy(x => (-x._2, -x._3))
        .map(x => x._1).take(algParameters.numOfLongVideoCluster4Recommend / 6)
      val selectJiluCluster = e._2._1.filter(x => !exposureClusters.contains(x._1) && x._1 >= 2560 && x._1 < 2893)
        .sortBy(x => (-x._2, -x._3))
        .map(x => x._1).take(algParameters.numOfLongVideoCluster4Recommend / 6)
      val selectComicCluster = e._2._1.filter(x => !exposureClusters.contains(x._1) && x._1 >= 2894 && x._1 < 3405)
        .sortBy(x => (-x._2, -x._3))
        .map(x => x._1).take(algParameters.numOfLongVideoCluster4Recommend / 6)
      val selectKidsCluster = e._2._1.filter(x => !exposureClusters.contains(x._1) && x._1 >= 3406 && x._1 < 3918)
        .sortBy(x => (-x._2, -x._3))
        .map(x => x._1).take(algParameters.numOfLongVideoCluster4Recommend / 6)

      val selectClusters = selectMovieCluster ++ selectTvCluster ++ selectZongyiCluster ++ selectJiluCluster ++ selectComicCluster ++ selectKidsCluster

      if (selectClusters.length < algParameters.numOfLongVideoCluster4Recommend) {
        (uid, selectClusters ++ exposureClusters.take(algParameters.numOfLongVideoCluster4Recommend - selectClusters.length))
      } else {
        (uid, selectClusters.distinct)
      }
    })
      .filter(e => e._2.length > 0)
      .persist(StorageLevel.DISK_ONLY)

    println("processedUserFeatures.count()")
    println(processedUserFeatures.count())

    //推荐长视频
    val recommendLongVideos = processedUserFeatures.map(e => {
      val uid = e._1
      var result = Array.empty[String]
      var i = 0
      e._2.foreach(x => {
        result = ArrayUtils.arrayAlternate(result, cluster2longVideoMap.getOrElse(x, Array.empty[String]), i, 1)
        i = i+1
      })
      (uid, result)
    }).
      filter(e => e._2.length > 0).
      persist(StorageLevel.DISK_ONLY)

    //使用曝光日志过滤
    val recommendLongVideosFilterExposed = recommendLongVideos
      .leftOuterJoin(frontPageExposedLongVideos).map(e => {
      val uid = e._1

      val longVideos4recommend = e._2._1
      val exposureVideos = e._2._2 match {
        case Some(x) => x
        case _ => Set.empty[String]
      }
      (uid, longVideos4recommend.filter(e => !exposureVideos.contains(e)))
    }).
      filter(e => e._2.length > 0).
      persist(StorageLevel.DISK_ONLY)

    //使用用户看过的日志过滤
    val recommendLongVideosFilterWatched = recommendLongVideosFilterExposed
      .leftOuterJoin(userWatchedLongVideos).map(e => {
      val uid = e._1

      val longVideos4recommend = e._2._1
      val watchedLongVideos = e._2._2 match {
        case Some(x) => x
        case _ => Array.empty[String]
      }

      (uid, longVideos4recommend.filter(e => !watchedLongVideos.contains(e)))
    }).
      filter(e => e._2.length > 0).
      persist(StorageLevel.DISK_ONLY)

    println("recommendLongVideos")
    println(recommendLongVideos.count())
    println("recommendLongVideosFilterExposed")
    println(recommendLongVideosFilterExposed.count())
    println("recommendLongVideosFilterWatched")
    println(recommendLongVideosFilterWatched.count())
    modelOutput.longVideoClusterDataFrame = recommendLongVideosFilterWatched.flatMap(x => x._2.map(y => (x._1, y))).toDF(modelOutput.uidColName, modelOutput.sidColName)
  }

  override protected def afterInvoke(): Unit = {

  }

}

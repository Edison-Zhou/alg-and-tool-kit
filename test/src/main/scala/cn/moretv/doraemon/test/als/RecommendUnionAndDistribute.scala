package cn.moretv.doraemon.test.als

import cn.moretv.doraemon.common.enum.{FileFormatEnum, FormatTypeEnum}
import cn.moretv.doraemon.common.path.{MysqlPath, CouchbasePath, HdfsPath, Path}
import cn.moretv.doraemon.common.util.{DateUtils, TransformUtils}
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.data.writer.{DataPack, DataPackParam, DataWriter, DataWriter2Kafka}
import cn.moretv.doraemon.test.constant.PathConstants
import cn.moretv.doraemon.test.{BaseClass , Utils}
import cn.whaley.sdk.dataOps.{HDFSOps}
import cn.whaley.sdk.utils.TransformUDF
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
  *  此类过时，将使用cn.moretv.doraemon.test.als.FrontPageRecommend类代替此类。
  *
  * @author wang.baozhi 
  * @since 2018/7/19 下午3:23 
  */
object RecommendUnionAndDistribute extends BaseClass {

  val numOfRecommend = 100

  //追剧暂时用原来的计算结果
  val pathOfMoretvSeriesChasingRecommend = "/ai/data/dw/moretv/seriesChasingRecommend"
  //byContentType暂时用原来的计算结果,应该使用ALSRecommend的计算结果来代替
  val pathOfMoretvALSResultByContentType = "/ai/data/dw/moretv/ALSResult/byContentType"

  val pathOfSelectMovies = "/ai/dw/moretv/similar/default/movie/defaultSimilarMovies.csv"

  val alsPath = "/ai/tmp/ALSResult"
  val numOfRecommend2User = 10
  val numOfRecommend2Kafka = 50

  override def execute(): Unit = {
    try {
      /**
        * 取三部分结果进行融合
        * 1、ALS推荐结果
        * 2、基于相似的强冲击影片
        * 3、基于用户高评分的单片推荐的相似影片
        */
      TransformUDF.registerUDFSS
      val alsRecommend =
        getRecommendWithoutScore(alsPath).
          map(e => (e._1, Utils.randomArray(e._2))).
          persist(StorageLevel.MEMORY_AND_DISK)

      //uid,sid 4685577
      val seriesChasingRecommend =
        getRecommendWithoutScore(pathOfMoretvSeriesChasingRecommend).
          map(e => (e._1, e._2.take(numOfRecommend))).persist(StorageLevel.MEMORY_AND_DISK)

      val similarityRecommend =
        getRecommendWithoutScore(PathConstants.tmp_similarity_dir).
          map(e => (e._1, e._2.take(numOfRecommend))).persist(StorageLevel.MEMORY_AND_DISK)

      val longVideoClusterRecommend =
        getRecommendWithoutScore(PathConstants.tmp_long_video_cluster_dir).
          map(e => (e._1, Utils.takeThenRandom(e._2,numOfRecommend))).persist(StorageLevel.DISK_ONLY)

      /**
        * 原始对照组: 首页今日推荐结果融合
        */
      val frontPageUnionRecommend = alsRecommend
        .leftOuterJoin(similarityRecommend)
        .leftOuterJoin(seriesChasingRecommend)
        .fullOuterJoin(longVideoClusterRecommend)
        .map(e => {
          val uid = e._1
          var ALS = Array.emptyIntArray
          var similarMovies = Array.emptyIntArray
          var seriesVideos = Array.emptyIntArray
          var clusterMovies = Array.emptyIntArray

          ALS = e._2._1 match {
            case Some(x) => x._1._1
            case _ => Array.emptyIntArray
          }
          similarMovies = e._2._1 match {
            case Some(x) => x._1._2 match {
              case Some(y) => y
              case _ => Array.emptyIntArray
            }
            case _ => Array.emptyIntArray
          }
          seriesVideos = e._2._1 match {
            case Some(x) => x._2 match {
              case Some(y) => y
              case _ => Array.emptyIntArray
            }
            case _ => Array.emptyIntArray
          }
          clusterMovies = e._2._2 match {
            case Some(x) => x
            case _ => Array.emptyIntArray
          }

          (uid, (ALS, similarMovies, seriesVideos, clusterMovies))
        })
        .filter(e => e._2._1.length > 0 || e._2._2.length > 0 || e._2._3.length > 0 || e._2._4.length > 0)
        .map(e => {
          val uid = e._1

          val ALS = e._2._1
          val similarMovies = e._2._2
          val seriesVideos = e._2._3
          val clusterMovies = e._2._4

          if (ALS.length > 0) {
            if (clusterMovies.length > 0 && seriesVideos.length > 0) {
              //cluster + series + similar + ALS + cluster + series
              // clusterMovies : seriesVideos : similarMovies : ALS = 3:1:1:1 (since seriesVideos.length almost equals 1)
              val union1 = Utils.arrayAlternate(clusterMovies, seriesVideos, 1, 1)
              val union2 = Utils.arrayAlternate(union1.distinct, similarMovies, 2, 1)
              val union3 = Utils.arrayAlternate(union2.distinct, ALS, 3, 1)

              (uid, (union3.distinct, "cluster_series_similar_ALS"))
            } else if (clusterMovies.length > 0) {
              //cluster + similar + cluster + ALS + similar + cluster
              // clusterMovies : similarMovies : ALS = 3:2:1
              val union1 = Utils.arrayAlternate(clusterMovies, similarMovies, 1, 1)
              val union2 = Utils.arrayAlternate(union1.distinct, ALS, 3, 1)

              (uid, (union2.distinct, "cluster_similar_ALS"))
            } else if (seriesVideos.length > 0) {
              //ALS + series + similar + ALS + series + similar
              // seriesVideos : similarMovies : ALS = 1:1:1
              val union1 = Utils.arrayAlternate(seriesVideos, ALS, 1, 1)
              val union2 = Utils.arrayAlternate(union1.distinct, similarMovies, 2, 1)

              (uid, (union2.distinct, "series_similar_ALS"))
            } else {
              //similar + ALS + similar + ALS + similar + ALS
              //similar : ALS = 1:1
              val union1 = Utils.arrayAlternate(ALS, similarMovies, 1, 1)

              (uid, (union1.distinct, "similar_ALS"))
            }
          } else {
            (uid, (clusterMovies, "cluster"))
          }

        })
        .filter(e => e._2._1.length > 0)
        .persist(StorageLevel.MEMORY_AND_DISK)

      val frontPageExposureLongVideos = getFrontPageExposureLongVideos(PathConstants.pathOfMoretvFrontPageExposureLongVideos, 1)
      // 各算法结果已经过滤过曝光，这里只过滤当天的实时曝光结果

      val frontPageRecommend4original = frontPageUnionRecommend.leftOuterJoin(frontPageExposureLongVideos)
        .map(e => {
          val uid = e._1
          val recommend = e._2._1._1
          val alg = e._2._1._2
          val exposure = e._2._2 match {
            case Some(x) => x
            case _ => Set.empty[Int]
          }
          val result = recommend.filter(x => !exposure.contains(x))
          (uid, (result, alg))
        }).persist(StorageLevel.MEMORY_AND_DISK)



      println("frontPageRecommend4original")
      println(frontPageRecommend4original.count())


      /**
        * 首页AB分组推荐
        */
      val frontPageABGroup = frontPageRecommend4original
        .map(e => {
          val uid = e._1
          val originalResult = e._2._1
          val alg = "original" + "." + e._2._2
          (uid, originalResult, alg)
        }).persist(StorageLevel.MEMORY_AND_DISK)


      /**
        * 生成会员看看的默认推荐
        */
      //RDD[(sid, (contentType, tags, risk_flag, supply_type))]
      val vipDefaultRecommend = getVideoData
        .filter(e => e._2._3 == 0 && e._2._4 == "vip")
        .map(e => e._1).collect()

      println(s"vipDefaultRecommend = ${vipDefaultRecommend.length}")

      /**
        * 首页vip推荐
        *
        * uid,sid,score 1084997800
        */
      val vipRecommendByALS = getRecommendWithScore(pathOfMoretvALSResultByContentType + "/vip")
        .map(e => (e._1, e._2.map(x => x._1)))

      val vipRecommend = vipRecommendByALS
        .leftOuterJoin(frontPageABGroup.map(e => (e._1, e._2)))
        .map(e => {
          val frontpageRecommend = e._2._2 match {
            case Some(x) => x
            case _ => Array.emptyIntArray
          }
          val result = e._2._1.filter(x => !frontpageRecommend.contains(x))

          (e._1, Utils.randomArray(result), "ALS")
        }).filter(e => e._2.length > 0)
        .persist(StorageLevel.MEMORY_AND_DISK)

      /**
        * 取默认结果
        */
      val path = new HdfsPath(pathOfSelectMovies, FileFormatEnum.CSV)
      val editorSelectMovies = DataReader.read(path).rdd.map(x => TransformUtils.transferSid(x.getString(0))).collect()
      val defaultRecommend = Utils.randomTake(editorSelectMovies, numOfRecommend2Kafka)
      println("defaultRecommend size:" + defaultRecommend.size)

      /**
        * 写入HDFS并同步kafka
        */
      val recommend2User = frontPageABGroup.map(e => (e._1, e._2.take(numOfRecommend2User)))

      //生产环境：/ai/dws/moretv/biz/homePage/rec/20180719/*
      //uid,sid，567612834
      recommend2HDFS(recommend2User,
        PathConstants.tmp_front_page_recommend + "/"
          + DateUtils.todayWithOutDelimiter + "/"
          + DateUtils.hour.toString)

      //生产环境：/ai/dws/moretv/biz/homePage/vip/20180719/*
      //uid,sid，325322833
      recommend2HDFS(vipRecommend.map(e => (e._1, e._2.take(numOfRecommend2User))),
        PathConstants.tmp_front_page_recommend_vip + "/"
          + DateUtils.todayWithOutDelimiter + "/"
          + DateUtils.hour.toString)

      //write to kafka
      defaultRecommend2Kafka4Couchbase("p:v:", Utils.randomArray(vipDefaultRecommend), "default", "couchbase-moretv-topic-test")
      defaultRecommend2Kafka4Couchbase("p:p:", defaultRecommend, "default_test", "couchbase-moretv-topic-test")
      recommend2Kafka4Couchbase("p:p:", frontPageABGroup, "couchbase-moretv-topic-test")
      recommend2Kafka4Couchbase("p:v:", vipRecommend, "couchbase-moretv-topic-test")
    } catch {
      case e: Exception => throw e
    }
  }

  /**
    * 默认推荐插入kafka
    *
    * @param servicePrefix 业务前缀
    * @param recommend     推荐结果
    * @param kafkaTopic    kafka topic
    */
  def defaultRecommend2Kafka4Couchbase(servicePrefix: String,
                                       recommend: Array[Int],
                                       alg: String,
                                       kafkaTopic: String): Unit = {
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._
    val param = new DataPackParam
    param.format = FormatTypeEnum.KV
    param.keyPrefix = servicePrefix
    param.extraValueMap = Map("date" -> DateUtils.getTimeStamp, "alg" -> alg)
    val path: Path = CouchbasePath(kafkaTopic)
    val dataWriter: DataWriter = new DataWriter2Kafka
    val recommendSid = recommend.take(numOfRecommend2Kafka)
    val list = List(("default", recommendSid))
    val dataSource = DataPack.pack(list.toDF("default", "id"), param)
    dataSource.collect().foreach(println)
    dataWriter.write(dataSource, path)
  }

  /**
    * 推荐结果插入kafka
    *
    * @param servicePrefix 业务前缀
    * @param recommend     推荐结果
    * @param kafkaTopic    kafka topic
    */
  def recommend2Kafka4Couchbase(servicePrefix: String,
                                recommend: RDD[(Long, Array[Int], String)],
                                kafkaTopic: String): Unit = {
    val sqlContextVar = spark.sqlContext
    import sqlContextVar.implicits._
    val param = new DataPackParam
    param.format = FormatTypeEnum.KV
    param.keyPrefix = servicePrefix
    param.extraValueMap = Map("date" -> DateUtils.getTimeStamp)

    val path: Path = CouchbasePath(kafkaTopic)
    val dataWriter: DataWriter = new DataWriter2Kafka
    val resultRdd = recommend.map(x => {
      val uid = x._1
      val rec = x._2.take(numOfRecommend2Kafka)
      val alg = x._3
      (uid, rec, alg)
    }
    )
    val dataSource = DataPack.pack(resultRdd.toDF("uid", "id", "alg"), param)
    dataWriter.write(dataSource, path)
  }

  /**
    * 获取不带评分的推荐结果
    *
    * @param path HDFS存储路径
    * @return RDD[(uid, Array[sid])]
    */
  def getRecommendWithoutScore(path: String): RDD[(Long, Array[Int])] = {
    if (HDFSOps.existsFile(path + "/Latest")) {
      println(s"read $path" + "/Latest")
      DataReader.read(new HdfsPath(path + "/Latest")).rdd.
        map(r => (r.getLong(0), r.getInt(1))).
        groupByKey().map(e => (e._1, e._2.toArray))
    } else {
      require(HDFSOps.existsFile(path + "/BackUp"), "The backUp result doesn't exist")
      DataReader.read(new HdfsPath(path + "/BackUp")).rdd.
        map(r => (r.getLong(0), r.getInt(1))).
        groupByKey().map(e => (e._1, e._2.toArray))
    }
  }


  /**
    * 获取首页曝光给用户的长视频
    *
    * @param path      路径
    * @param numOfDays 天数
    * @return RDD[(uid, Set[sid])]
    */
  def getFrontPageExposureLongVideos(path: String, numOfDays: Int): RDD[(Long, Set[Int])] = {
    require(HDFSOps.existsFile(path), "The FrontPage Exposure doesn't exist")
    val startDay = DateUtils.farthestDayWithOutDelimiter(numOfDays)
    val today = DateUtils.farthestDayWithOutDelimiter(0)
    println(s"startDay = $startDay")
    println(s"today = $today")
    DataReader.read(new HdfsPath(path)).filter(s"key_time > '$startDay' and key_time <= '$today'").rdd
      .map(r => (r.getLong(0), r.getString(1).toInt))
      .groupByKey()
      .map(r => (r._1, r._2.toArray.distinct.toSet))
  }

  /**
    * 用于从节目库获取标签信息
    * 只推正片
    * RDD[(sid, (contentType, tags, risk_flag, supply_type))]
    */
  def getVideoData = {
    val videoDataPath: MysqlPath = new MysqlPath("bigdata-appsvr-130-4", 3306,
      "tvservice", "mtv_program", "bislave", "slave4bi@whaley",
      Array("sid", "episodeCount", "contentType", "tags", "risk_flag", "supply_type"),
      " ((contentType in ('tv','zongyi','comic','kids','jilu') and videoType = 1) || " +
        " (contentType = 'movie' and videoType = 0)) " +
        " and type = 1 and status = 1 ")
    val videoData = DataReader.read(videoDataPath)
    println("videoData.printSchema():"+videoData.printSchema())
    videoData.rdd.map(r=>(r.getString(0), r.getInt(1), r.getString(2), r.getString(3), r.getInt(4), r.getString(5))).
      filter(x => if (x._3.equals("kids")) x._2 > 0 else true).
      map(x => (TransformUDF.transferSid(x._1), (x._3, x._4, x._5, x._6)))

    /*val sql = "SELECT sid,episodeCount,contentType,tags, risk_flag, supply_type FROM " + "mtv_program" +
      " WHERE ID >= ? AND ID <= ? " +
      " and ((contentType in ('tv','zongyi','comic','kids','jilu') and videoType = 1) || " +
      " (contentType = 'movie' and videoType = 0)) " +
      " and type = 1 and status = 1 "
    val mysqlOps = new MySqlOps("moretv_recommend_mysql")
    mysqlOps.getJdbcRDD(sc, sql, "mtv_program", (r: ResultSet) => (r.getString(1), r.getInt(2), r.getString(3), r.getString(4), r.getInt(5), r.getString(6)), 20).
      filter(x => if (x._3.equals("kids")) x._2 > 0 else true).
      map(x => (TransformUDF.transferSid(x._1), (x._3, x._4, x._5, x._6)))
      */
  }

  /**
    * 获取带评分的推荐结果
    *
    * @param path HDFS存储路径
    * @return RDD[(uid, Array[(sid, score)])]
    */
  def getRecommendWithScore(path: String): RDD[(Long, Array[(Int, Double)])] = {
    if (HDFSOps.existsFile(path + "/Latest")) {
      println(s"read $path" + "/Latest")
      DataReader.read(new HdfsPath(path + "/Latest")).rdd.
        map(r => (r.getLong(0), (r.getInt(1), r.getDouble(2)))).
        groupByKey().map(e => (e._1, e._2.toArray))
    } else {
      require(HDFSOps.existsFile(path + "/BackUp"), "The backUp result doesn't exist")
      DataReader.read(new HdfsPath(path + "/BackUp")).rdd.
        map(r => (r.getLong(0), (r.getInt(1), r.getDouble(2)))).
        groupByKey().map(e => (e._1, e._2.toArray))
    }
  }

  /**
    * 不带评分的推荐结果写入HDFS
    *
    * @param recommend RDD[(uid, Array[sid])]
    * @param path      HDFS路径
    */
  def recommend2HDFS(recommend: RDD[(Long, Array[Int])], path: String): Unit = {
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._
    val data = recommend.flatMap(x => x._2.map(y => (x._1, y))).toDF("uid", "sid")

    HDFSOps.deleteHDFSFile(path)
    data.write.parquet(path)
  }

}

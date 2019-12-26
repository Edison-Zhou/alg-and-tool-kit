package cn.moretv.doraemon.test

import cn.moretv.doraemon.algorithm.als.AlsModel
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.path.{DateRange, HdfsPath, HivePath, MysqlPath}
import cn.moretv.doraemon.common.util.DateUtils
import cn.moretv.doraemon.data.writer.DataWriter2Hdfs
import cn.moretv.doraemon.test.constant.PathConstants
import cn.whaley.sdk.algorithm.{TopN, VectorFunctions}
import cn.whaley.sdk.utils.TransformUDF
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
  * Created by cheng_huan on 2018/7/9.
  * 将使用cn.moretv.doraemon.test.als.ALSRecommend替代此类
  */
object ALSRecommend {

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
    * @param userFactors ALS训练的用户特征
    * @param itemFactors ALS训练的视频特征
    * @param topN 推荐视频数目
    * @param videoDataDF 视频数据
    * @param weighedVideos 需要加权的视频
    * @param contentType 视频类型（all代表所有长视频）
    * @param userRiskFlagDF 用户风险等级
    * @param userWatchedDF 用户看过的视频
    * @param userExposuredDF 曝光给用户过的视频
    * @return DataFrame[(uid, Array[(sid, score)])]
    */
  def ALSRecommend(userFactors:DataFrame,
                   itemFactors:DataFrame,
                   topN: Int,
                   videoDataDF: DataFrame,
                   weighedVideos: Map[Int, Double],
                   contentType: String,
                   userRiskFlagDF: DataFrame,
                   userWatchedDF: DataFrame,
                   userExposuredDF: DataFrame):DataFrame = {
    val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    val sc: SparkContext = ss.sparkContext
    import ss.implicits._
    //DataFrame2RDD
    val videoData = videoDataDF.rdd
      .map(r => (r.getString(0),r.getInt(1),r.getString(2),r.getString(3), r.getInt(4), r.getString(5)))
      .filter(x => if(x._3.equals("kids")) x._2 > 0 else true)
      .map(x => (TransformUDF.transferSid(x._1), (x._3, x._4, x._5, x._6)))
      .persist(StorageLevel.MEMORY_AND_DISK)
    val userRiskFlag = userRiskFlagDF.rdd
      .map(r => (TransformUDF.calcLongUserId(r.getString(0)), r.getInt(1)))
    val userWatched = userWatchedDF.rdd
      .map(r => (r.getLong(0), r.getString(1).toInt))
      .groupByKey()
      .map(e => (e._1, e._2.toArray))
    val userExposured = userExposuredDF.rdd
      .map(r => (r.getLong(0), r.getString(1).toInt))
      .groupByKey()
      .map(e => (e._1, e._2.toArray))

    //有效视频合集
    var validVideoData = videoData.map(e => (e._1, e._2._3))

    if(contentType != "all") {
      validVideoData = videoData.filter(e => e._2._1 == contentType).map(e => (e._1, e._2._3))
    }

    //提取视频特征矩阵
    val itemFactorsRDD = itemFactors.rdd.
      map(x => (x.getInt(0),x.getSeq[Double](1).toArray)).
      map(x => {
        val sid = x._1
        val weight = weighedVideos.getOrElse(sid, 1.0)

        (sid, x._2.map(y => (weight * y).toFloat))
      })

    //提取用户特征矩阵
    val userFactorsRDD = userFactors.rdd.
      map(x=>(x.getLong(0),x.getSeq[Double](1).toArray.map(e => e.toFloat)))
      .filter(e => e._1 % 2 == 1)

    //用户因子矩阵和视频因子矩阵扩展
    val userVectors = userFactorsRDD.leftOuterJoin(userRiskFlag).map(e => {
      val uid = e._1
      val userFactor = e._2._1
      val userRiskFlag = e._2._2 match {
        case Some(x) => x
        case _ => 0
      }

      (uid, (userFactor, userRiskFlag))
    })

    //过滤有效视频
    val itemVectors = itemFactorsRDD.join(validVideoData).map(e => {
      val sid = e._1
      val itemFactor = e._2._1
      val videoRiskFlag = e._2._2

      (sid, (itemFactor, videoRiskFlag))
    })

    // BroadCast为只读变量，避免运算中对其复制，从而加快运行速度
    val bcItemVectors = sc.broadcast(itemVectors.collect())

    //得到给用户推荐的视频
    val recommends = {
      //推荐视频结果
      userVectors.leftOuterJoin(userWatched)
        .leftOuterJoin(userExposured)
        .repartition(2000)
        .mapPartitions(partition=>{
          lazy val order = new OrderByScore
          val result = partition.map(x=>{
            val uid = x._1
            val userRiskFlag = x._2._1._1._2

            val userWatched = x._2._1._2 match {
              case Some(y) => y.toSet
              case _ => Set.empty[Int]
            }

            val exposures2user = x._2._2 match {
              case Some(y) => y.toSet
              case _ => Set.empty[Int]
            }
            println("==========================")
            println(bcItemVectors.value.size)
            println(userWatched.size)
            println(exposures2user.size)

            val available = bcItemVectors.value.filter(y => {
              val sid = y._1
              val videoRiskFlag = y._2._2

              !userWatched.contains(sid) && !exposures2user.contains(sid) && (videoRiskFlag + userRiskFlag <= 2)
            })

            val likeV = available.
              map(y=> (y._1,VectorFunctions.denseProduct(y._2._1,x._2._1._1._1)))

            (uid,TopN.apply(topN,likeV)(order).toArray)
          })

          result.map(e => (e._1, e._2.take(topN)))
        })
    }
    recommends.flatMap(e => e._2.map(x => (e._1, x._1, x._2.toDouble)))
      .toDF("uid", "sid", "score")
  }

  def main(args: Array[String]): Unit = {
    try {
      val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
      import ss.implicits._
      //读取HDFS数据
      val numDaysOfWatched = 300
      val startDate = DateUtils.farthestDayWithDelimiter(numDaysOfWatched)
      val endDate = DateUtils.todayWithDelimiter
      val userWatchedPath: HdfsPath = new HdfsPath(PathConstants.pathOfMoretvLongVideoHistory,
        s"select userid, sid_or_subject_code from tmp where latest_optime >= '$startDate' and latest_optime <= '$endDate'")
      val numDaysOfExposure = new DateRange("yyyyMMdd",-90)
      val frontPageExposurePath: HdfsPath = new HdfsPath(numDaysOfExposure,
        PathConstants.pathOfMoretvFrontPageExposureLongVideos + "/key_time=#{date}",
        "select userid, sid from tmp")

      val userWatched = DataReader.read(userWatchedPath)
      println("userWatchedDF")
      userWatched.printSchema()
      userWatched.show(10, false)
      val userExposured = DataReader.read(frontPageExposurePath)
      println("userExposuredDF")
      userExposured.printSchema()
      userExposured.show(10, false)

      //读取MySQL数据
      val videoDataPath: MysqlPath = new MysqlPath("bigdata-appsvr-130-4", 3306,
        "tvservice", "mtv_program", "bislave", "slave4bi@whaley",
        Array("sid", "episodeCount", "contentType", "tags", "risk_flag", "supply_type"),
        "((contentType in ('tv','zongyi','comic','kids','jilu') and videoType = 1) || " +
          " (contentType = 'movie' and videoType = 0)) " +
          " and type = 1 and status = 1 ")
      val videoData = DataReader.read(videoDataPath)
      println("videoDataDF")
      videoData.printSchema()
      videoData.show(10, false)

      /**
        * 获取“编辑精选”标签的电影
        */
      val tagDataPath: MysqlPath = new MysqlPath("bigdata-appsvr-130-6", 3306,
        "europa", "tag_program_mapping", "bislave", "slave4bi@whaley", Array("sid"),
        "tag_id = 152063")
      val selectMovies = DataReader.read(tagDataPath)
      println("selectMovieDF")
      selectMovies.printSchema()
      selectMovies.show(10, false)
      val weightVideos = selectMovies.rdd.map(r => (r.getString(0)))
        .map(e => (TransformUDF.transferSid(e), PathConstants.weightOfSelectMovies))
        .collectAsMap().toMap

      //load ALSmodel
      val alsModel = new AlsModel()
      //val alsModelPath = new HdfsPath(PathConstants.tmp_als_model_dir)
//      alsModel.loadModel(alsModelPath)
      val userFactor = alsModel.matrixU
      val itemFactor = alsModel.matrixV

      /**
        * 获取用户风险等级
        */
      val userRiskPath = new HivePath("select a.user_id, b.dangerous_level " +
        "from dw_dimensions.dim_medusa_terminal_user a left join dw_dimensions.dim_web_location b " +
        "on a.web_location_sk = b.web_location_sk " +
        "where a.dim_invalid_time is null and b.dangerous_level > 0")
      val userRisk = DataReader.read(userRiskPath)
      /*val cityDangerousLevelPath1 = new HivePath("select city_name, dangerous_level from dw_dimensions.dim_medusa_city_dangerous_level")
      val cityDangerousLevel = HiveReader.read(cityDangerousLevelPath1)
      /*val cityDangerousLevelPath = new HdfsPath(PathConstants.pathOfCityDangerousLevel)
      val cityDangerousLevel = DataReader.read(cityDangerousLevelPath).select("city_name", "dangerous_level")*/
      val userCityLevelPath = new HdfsPath(PathConstants.pathOfUserCityLevel + DateUtils.farthestDayWithOutDelimiter(3) + "/00")
      val userCityLevel = DataReader.read(userCityLevelPath).select("user_id", "city")

      val userRiskFlag = userCityLevel
        .join(cityDangerousLevel, userCityLevel("city") === cityDangerousLevel("city_name"), "left")
        .rdd.map(r => (r.getString(0), r.getString(2)))
        .map(e => {
          val uid = TransformUDF.calcLongUserId(e._1)
          val risk = e._2 match {
            case "a" => 2
            case "b" => 1
            case _ => 0
          }
          (uid, risk)
        }).toDF()*/

      println("userRiskFlag")
      userRisk.printSchema()
      userRisk.show(10, false)

      //ALS Recommend
      val ALSResult = ALSRecommend(userFactor, itemFactor, 100,
        videoData, weightVideos, "all", userRisk, userWatched, userExposured)

      println("ALSResult")
      //ALSResult.count()
      ALSResult.printSchema()
      //ALSResult.show(10, false)

      /**
        * 写入HDFS并同步kafka
        */
      new DataWriter2Hdfs().write(ALSResult, new HdfsPath("/ai/tmp/ALSResult"))

    } catch {
      case e: Exception => throw e
    }
  }
}

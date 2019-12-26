package cn.moretv.doraemon.test.util

import cn.moretv.doraemon.common.constant.Constants
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.FormatTypeEnum
import cn.moretv.doraemon.common.path._
import cn.moretv.doraemon.common.util.{ArrayUtils, DateUtils, TransformUtils}
import cn.moretv.doraemon.data.writer.{DataPack, DataWriter2Kafka, DataWriter, DataPackParam}
import cn.moretv.doraemon.test.constant.PathConstants
import cn.whaley.sdk.dataOps.HDFSOps
import cn.whaley.sdk.utils.TransformUDF
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

/**
  *
  * @author wang.baozhi 
  * @since 2018/7/26 下午3:40 
  */
object BizUtils {

  /**
    * 过滤地域屏蔽
    * 要求输入的DataFrame中包含uid，sid这两列
    *
    * @param recommend  格式为：DataFrame[Row(uid,sid, 其他列....)],recommend可能包含不止uid,sid的列
    * @return DataFrame  recommend的子集
    */
  def filterRiskFlag(recommend: DataFrame): DataFrame = {
    val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    import ss.implicits._
    //获取用户风险等级
    val userRiskPath = new HivePath("select a.user_id, b.dangerous_level " +
      "from dw_dimensions.dim_medusa_terminal_user a left join dw_dimensions.dim_web_location b " +
      "on a.web_location_sk = b.web_location_sk " +
      "where a.dim_invalid_time is null and b.dangerous_level > 0")
    val userRisk = DataReader.read(userRiskPath).map(e=>(TransformUtils.calcLongUserId(e.getString(0)),e.getInt(1))).toDF("uid","userRisk")
    getDataFrameInfo(userRisk,"userRisk")

    //获取视频风险等级
    val videoRiskPath: MysqlPath = new MysqlPath("bigdata-appsvr-130-4", 3306,
      "tvservice", "mtv_program", "bislave", "slave4bi@whaley",
      Array("sid","risk_flag"),
      "((contentType in ('tv','zongyi','comic','kids','jilu') and videoType = 1) || " +
        " (contentType = 'movie' and videoType = 0)) " +
        " and type = 1 and status = 1 ")
    val videoRisk = DataReader.read(videoRiskPath).map(e=>(TransformUtils.transferSid(e.getString(0)),e.getInt(1))).toDF("sid","videoRisk")
    getDataFrameInfo(videoRisk,"videoRisk")

    val finalRecommend=recommend.join(userRisk, recommend("uid") === userRisk("uid"), "left")
      .join(videoRisk, recommend("sid") === videoRisk("sid"), "left")
      .where(expr("if(userRisk is null,0,userRisk)+if(videoRisk is null,0,videoRisk) <= 2"))
      .select(recommend("*"))

    finalRecommend
  }

  /**
    * 读取moretv长视频有效节目
    *
    * @return DataFrame[sid]
    */
  def getValidLongVideoSid(): DataFrame = {
    val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    import ss.implicits._
    val videoDataPath: MysqlPath = new MysqlPath("bigdata-appsvr-130-4", 3306,
          "tvservice", "mtv_program", "bislave", "slave4bi@whaley",
          Array("sid", "episodeCount", "contentType"),
          "((contentType in ('tv','zongyi','comic','kids','jilu') and videoType = 1) || " +
            " (contentType = 'movie' and videoType = 0)) " +
            " and type = 1 and status = 1 ")
        val videoData = DataReader.read(videoDataPath).map(r=>(r.getString(0), r.getInt(1), r.getString(2))).
          filter(x => if (x._3.equals("kids")) x._2 > 0 else true)
          .map(e=>TransformUtils.transferSid(e._1)).toDF("sid")
        videoData
  }


  def getDataFrameWithDataRange(path: String, numOfDays: Int): DataFrame = {
    val dateRange = new DateRange("yyyyMMdd",-numOfDays)
    DataReader.read(new HdfsPath(dateRange,path))
  }

  def getDataFrameNewest(path: String): DataFrame = {
    if (HDFSOps.existsFile(path + "/Latest")) {
      println(s"read $path" + "/Latest")
      DataReader.read(new HdfsPath(path+ "/Latest"))
    } else {
      require(HDFSOps.existsFile(path + "/BackUp"), "The backUp result doesn't exist")
      DataReader.read(new HdfsPath(path+ "/BackUp"))
    }
  }

  /**
    *
    * 将uid,sid按照uid进行group by，获得sid的数组，然后随机打乱数组顺序
    *
    * @param dataFrame DataFrame
    * @param numOfRecommend  取多少sid
    * @param operation  数组操作方式
    * @return DataFrame
    */
  def getUidSidDataFrame(dataFrame: DataFrame,numOfRecommend:Int,operation:String): DataFrame = {
    val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    import ss.implicits._
    var df:DataFrame=null
    val rdd=dataFrame.rdd.
      map(r => (r.getLong(0), r.getInt(1))).
      groupByKey().map(e => (e._1, e._2.toArray))

    if(operation.equalsIgnoreCase(Constants.ARRAY_OPERATION_TAKE)){
      df=rdd.map(e => (e._1, e._2.take(numOfRecommend))).flatMap(e => e._2.map(x => (e._1, x))).toDF("uid", "sid")
    }else if(operation.equalsIgnoreCase(Constants.ARRAY_OPERATION_RANDOM)){
      df=rdd.map(e => (e._1, ArrayUtils.randomArray(e._2.toArray))).flatMap(e => e._2.map(x => (e._1, x))).toDF("uid", "sid")
    }else if(operation.equalsIgnoreCase(Constants.ARRAY_OPERATION_TAKE_AND_RANDOM)){
      df=rdd.map(e => (e._1, ArrayUtils.takeThenRandom(e._2.toArray,numOfRecommend))).flatMap(e => e._2.map(x => (e._1, x))).toDF("uid", "sid")
    }
    df
  }

  /**
    *
    * 将uid,sid按照uid进行group by，获得sid的数组，然后随机打乱数组顺序
    *
    * @param base DataFrame
    * @param whiteOrBlack  黑名单或白名单
    * @param operation  left or right
    * @param blackOrWhite  过滤方式
    * @return DataFrame
    */
  def uidSidFilter(base: DataFrame,whiteOrBlack: DataFrame,operation:String,blackOrWhite:String): DataFrame = {
    var df:DataFrame=null
    if(operation.equalsIgnoreCase("left") && blackOrWhite.equalsIgnoreCase("black")){
      df=base.as("a").join(whiteOrBlack.as("b"), expr("a.uid = b.uid") && expr(s"a.sid = b.sid"), operation)
        .where("b.uid is null and b.sid is null")
        .selectExpr("a.*")
    }
    df
  }



  /**
    *
    * 获取节目会员正片
    *
    * @param riskFlag 输入riskFlag，如果为
    */
  def getVipSid(riskFlag:Int) : DataFrame = {
    val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    import ss.implicits._
    val videoDataPath: MysqlPath = new MysqlPath("bigdata-appsvr-130-4", 3306,
      "tvservice", "mtv_program", "bislave", "slave4bi@whaley",
      Array("sid", "episodeCount", "contentType", "tags", "risk_flag", "supply_type"),
      " ((contentType in ('tv','zongyi','comic','kids','jilu') and videoType = 1) || " +
        " (contentType = 'movie' and videoType = 0)) " +
        " and type = 1 and status = 1 ")
    val videoData = DataReader.read(videoDataPath)
    val rddFromMysql=videoData.rdd.map(r=>(r.getString(0), r.getInt(1), r.getString(2), r.getString(3), r.getInt(4), r.getString(5))).
      filter(x => if (x._3.equals("kids")) x._2 > 0 else true).
      map(x => (TransformUDF.transferSid(x._1), (x._3, x._4, x._5, x._6)))
    //RDD[(sid, (contentType, tags, risk_flag, supply_type))]
    if(riskFlag > -1){
      rddFromMysql.filter(e => e._2._3 == riskFlag && e._2._4 == "vip")
        .map(e => e._1).toDF("sid")
    }else{
      rddFromMysql.filter(e => e._2._4 == "vip")
        .map(e => e._1).toDF("sid")
    }
  }


  def rowNumber(df:DataFrame,partitionByName:String,orderByName:String,takeNumber:Int,isDropOrderByName:Boolean): DataFrame ={
      val dropColumns = new ArrayBuffer[String]()
      dropColumns+="temp"
      if(isDropOrderByName){
        dropColumns+=orderByName
      }
      df.withColumn("temp", row_number.over(Window.partitionBy(partitionByName).orderBy(col(orderByName).desc)))
      .filter("temp < "+takeNumber).drop(dropColumns.toSeq:_*)
  }

  def getDataFrameInfo(df: DataFrame,name:String): Unit = {
    println(s"$name.count():"+df.count())
    println(s"$name.printSchema:")
    df.printSchema()
  }



  /**
    * 推荐结果插入kafka
    *
    * @param servicePrefix 业务前缀
    * @param df     推荐结果
    * @param alg    算法名称
    * @param kafkaTopic    kafka topic
    */
  def recommend2Kafka4Couchbase(servicePrefix: String,df:DataFrame,alg: String, kafkaTopic: String): Unit = {
    val param = new DataPackParam
    param.format = FormatTypeEnum.KV
    param.keyPrefix = servicePrefix
    param.extraValueMap = Map("date" -> DateUtils.getTimeStamp, "alg" -> alg)
    val path: Path = CouchbasePath(kafkaTopic)
    val dataWriter: DataWriter = new DataWriter2Kafka
    val result=df.groupBy("uid").agg(collect_list("sid")).toDF("uid","id")
    val dataSource = DataPack.pack(result, param)
    dataWriter.write(dataSource, path)
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
                                       kafkaTopic: String,numOfRecommend2Kafka:Int): Unit = {
    val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    import ss.implicits._
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



  /**用于读取出活跃&&筛选出活跃用户
 *
    * @return DataFrame[user]
    * */
  def readActiveUser(): DataFrame = {
    val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    import ss.implicits._
    val df = getDataFrameNewest(PathConstants.pathOfMoretvActiveUser)
    val rdd=df.rdd.map(r => (r.getLong(0), r.getInt(1), r.getInt(2), r.getInt(3), r.getInt(4)))
             .filter(e => (e._5 >= 2 || e._4 >= 1 || e._3 >= 1 || e._2 >= 1 )).map(e => (e._1))
    val finalDataFrame = rdd.toDF("user")
    finalDataFrame
  }

  /** 用于获取评分数据
    *
    * @return DataFrame[user,item,rating]
    * */
  def readUserScore(path: String, numOfDays: Int): DataFrame= {
    val endDate = DateUtils.todayWithDelimiter
    val startDate = DateUtils.farthestDayWithDelimiter(numOfDays)
    val df = DataReader.read(new HdfsPath(path))
    val result = df.filter(s"latest_optime >= '$startDate' and latest_optime <= '$endDate'").
      selectExpr("userid as user", "cast(sid_or_subject_code as int ) as item","score as rating")
    result
  }



  /** 获取“编辑精选”标签的电影，并赋予权重
    *
    * @return Map[Int,Double]
    * */
  def sidFromEditor(weightOfSelectMovies:Double):Map[Int,Double]={
    val tagDataPath: MysqlPath = new MysqlPath("bigdata-appsvr-130-6", 3306,
      "europa", "tag_program_mapping", "bislave", "slave4bi@whaley", Array("sid"),
      "tag_id = 152063")
    val selectMovies = DataReader.read(tagDataPath)
    BizUtils.getDataFrameInfo(selectMovies,"selectMovies")
    val weightVideos = selectMovies.rdd.map(r => (r.getString(0)))
      .map(e => (TransformUDF.transferSid(e), weightOfSelectMovies))
      .collectAsMap().toMap
    weightVideos
  }



}

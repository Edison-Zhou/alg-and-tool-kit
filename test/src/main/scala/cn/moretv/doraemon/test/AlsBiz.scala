package cn.moretv.doraemon.test

import cn.moretv.doraemon.algorithm.als.{AlsAlgorithm, AlsModel}
import cn.moretv.doraemon.common.path.HdfsPath
import cn.moretv.doraemon.common.util.{DateUtils, HdfsUtil}
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.test.constant.PathConstants
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

/**
  *
  * Created by baozhiwang on 2018/6/11.
  * ALS算法biz，用来生成ALS Model并存储到HDFS
  *
  */
object AlsBiz extends BaseClass {
  //评分数据
   val pathOfMoretvLongVideoScore = "/user/hive/warehouse/ai.db/dw_base_behavior/product_line=moretv/partition_tag=V1/content_type={movie,tv,zongyi,jilu,comic,kids}/*"
  //活跃用户数据
  val pathOfMoretvActiveUser = "/ai/data/dw/moretv/userBehavior/activeUser"

  override def execute(): Unit = {

    // 算法结果数据存储路径
    val alsBasePath = PathConstants.tmp_als_model_dir

    // 用于获取评分数据
    val userScore = readUserScore

    // 用于筛选出活跃 
    val activeUser = readActiveUser

    // 算法部分
    println("log 算法部分:")
    val alsAlgorithm = new AlsAlgorithm()
    //改为DataFrame[Row(user,item,rating)]
    val inputData = userScore.alias("userScore").join(activeUser.alias("activeUser"),"user")
      .selectExpr("userScore.user as user", "userScore.item as item","userScore.rating as rating")
    //val inputData = userScore.toDF("user", "item", "rating")
    println("log inputData.schema:"+inputData.schema)
    println("log inputData.show:"+inputData.show())
    val dataMap = Map(alsAlgorithm.INPUT_DATA_KEY -> inputData)
    alsAlgorithm.initInputData(dataMap)
    alsAlgorithm.run()

    //打印20条结果
    println("log 打印20条结果:")
    alsAlgorithm.getOutputModel.asInstanceOf[AlsModel].matrixU.printSchema()
    alsAlgorithm.getOutputModel.asInstanceOf[AlsModel].matrixU.show
    alsAlgorithm.getOutputModel.asInstanceOf[AlsModel].matrixV.printSchema()
    alsAlgorithm.getOutputModel.asInstanceOf[AlsModel].matrixV.show

    //保存模型数据到HDFS
    println("log 保存模型数据到HDFS:")
//    alsAlgorithm.modelOutput.saveModel(new HdfsPath(alsBasePath))

    //测试load数据
     /*println("log 测试load数据:")
     val alsAlgorithmLoad = new AlsAlgorithm()
     alsAlgorithmLoad.getOutputModel.asInstanceOf[AlsModel].loadModel(new HdfsPath(alsBasePath))*/
  }


  def readUserScore(): DataFrame = {
    val numOfDays = 300
    val userScore = getHDFSScoredData(pathOfMoretvLongVideoScore, numOfDays)
    println("log userScore.schema:"+userScore.schema)
    println("log userScore.count():"+userScore.count())
    userScore.take(10).foreach(println)
    userScore
  }

  def getHDFSScoredData(path: String, numOfDays: Int): DataFrame= {
    val endDate = DateUtils.todayWithDelimiter
    val startDate = DateUtils.farthestDayWithDelimiter(numOfDays)
    println(s"startDate = $startDate")
    println(s"endDate = $endDate")
    val df = DataReader.read(new HdfsPath(path))
    println("log getHDFSScoredData:"+df.schema)

   val result = df.filter(s"latest_optime >= '$startDate' and latest_optime <= '$endDate'").
     selectExpr("userid as user", "cast(sid_or_subject_code as int ) as item","score as rating")
     result
  }

  /**用于读取出活跃 */
  def readActiveUser(): DataFrame = {
    val result = getActiveUser(pathOfMoretvActiveUser)
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._
    val finalDataFrame = result.toDF("user", "flag")
    println("finalDataFrame.schema:"+finalDataFrame.schema)
    println("finalDataFrame.count():"+finalDataFrame.count())
    finalDataFrame.take(10).foreach(println)
    finalDataFrame
  }

  /**
    * 用于筛选出活跃
    *
    * @param path 路径
    * @return  RDD[(Long, Int)]
    */
  def getActiveUser(path: String): RDD[(Long, Int)] = {
    var df: DataFrame = null
    if (HdfsUtil.pathIsExist(path + "/Latest")) {
       println(s"read $path" + "/Latest")
       df = DataReader.read(new HdfsPath(path + "/Latest"))
     } else {
       require(HdfsUtil.pathIsExist(path + "/BackUp"), "The backUp result doesn't exist")
       println(s"read $path" + "/BackUp")
       df = DataReader.read(new HdfsPath(path + "/BackUp"))
    }
      df.rdd. map(r => (r.getLong(0), r.getInt(1), r.getInt(2), r.getInt(3), r.getInt(4))).
      filter(e => (e._5 >= 2 || e._4 >= 1 || e._3 >= 1 || e._2 >= 1)).map(e => (e._1, 1))
  }

}
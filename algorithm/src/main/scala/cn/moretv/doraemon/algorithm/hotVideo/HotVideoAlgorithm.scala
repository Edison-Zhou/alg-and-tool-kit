package cn.moretv.doraemon.algorithm.hotVideo

import java.text.SimpleDateFormat
import java.util.Date

import cn.moretv.doraemon.common.alg.Algorithm
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by Edison_Zhou on 2019/2/22.
  * 算法说明：抽象出一个获取热门影片的算法模块,以供搜索界面"大家都在搜"和"similarDefault"等用到热门影片数据的模块使用
  *
  * 算法逻辑: 算法调用者须从HDFS路径中传入(dr，hdfsPath, sql)读取近N天的影片每日评分数据,
  * 获取sid_or_subject_code as sid, optime, content_type, score字段组成的四列DF,构成输入此算法的baseScoreDF,
  * 对其中score大于评分阈值的sid进行播放次数统计,并通过当前时间与min(optime)的差值计算近N天内影片首次有播放记录距今的小时数,
  * 以播放次数/小时数作为节目的popularity, 由高到低排列, 取前hotNum个播放次数最多的sid,
  * 附带其对应类型和popularity, 得到hotDF供各Biz中的相关模块使用
  */
class HotVideoAlgorithm extends Algorithm{

  //数据Map的Key定义
  val INPUT_BASESCORE_KEY = "baseScore"
  //输入输出属性定义
  val algParameters: HotVideoParameters = new HotVideoParameters()
  val modelOutput: HotVideoModel = new HotVideoModel()


  //进行参数初始化设置
  override def beforeInvoke(): Unit = {
    dataInput.get(INPUT_BASESCORE_KEY) match {
      case None => throw new IllegalArgumentException("未设置输入数据")
      case _ =>
    }
  }


  //算子核心处理逻辑
  override def invoke(): Unit = {
    val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    import ss.implicits._
    val sc: SparkContext = ss.sparkContext

    val TEMP_RANK = "temp_col_rank"
    val FIRST_OPTIME = "first_optime"

    //将当前时间转换为指定形式的时间戳
    val sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val presentString = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())
    val presentTimestamp = sdf.parse(presentString).getTime

    //数据格式预处理
    //原始评分dataframe, 须由content_type, sid, optime, score四列构成, 列顺序不严格限制
    var baseScoreDF = dataInput(INPUT_BASESCORE_KEY)

    //根据传入的待获取的热门影片的类型做筛选
    baseScoreDF = algParameters.contentType match {
      case "all" => baseScoreDF
      case _ => baseScoreDF.filter(algParameters.typeColumn + "=" + algParameters.contentType)
    }

    /*
    对评分大于分数阈值的sid统计对应个数, 附带其content_type信息
    sidTypeCountDF = DF(sid, content_type, count)
    */
    val sidTypeCountDF = baseScoreDF.filter(algParameters.scoreColumn + s" > ${algParameters.scoreThreshold}")
      .groupBy(algParameters.videoColumn, algParameters.typeColumn).count()

    //得到每个影片首次有播放记录的时间
    val firstOptimeDF = baseScoreDF.groupBy(algParameters.videoColumn).agg(algParameters.optimeColumn -> "min")
      .toDF(algParameters.videoColumn, FIRST_OPTIME)

    //计算影片首次有播放记录距今的时间间隔, durationDF = DF(sid, duration)
    val durationDF = firstOptimeDF.map(r => (r.getAs[String](algParameters.videoColumn),
      (presentTimestamp - sdf.parse(r.getAs[String](FIRST_OPTIME)).getTime)/algParameters.millisecondPerTimeUnit))
      .toDF(algParameters.videoColumn, algParameters.durationColumn)

    // 通过影片的播放次数与时间跨度之商得到影片热度，
    // 根据热度降序排列"content_type", "sid", "popularity"构成的热门有序影片dataframe
    val hotDF = sidTypeCountDF.join(durationDF, sidTypeCountDF(algParameters.videoColumn)===durationDF(algParameters.videoColumn))
      .withColumn(algParameters.popularityColumn, col(algParameters.countColumn)/(col(algParameters.durationColumn) + 0.1))
      .drop(durationDF(algParameters.videoColumn)).drop(algParameters.countColumn, algParameters.durationColumn)


    val hotOrderedDF = hotDF.withColumn(TEMP_RANK, row_number().over(Window.orderBy(col(algParameters.popularityColumn).desc)))
      .filter(TEMP_RANK + "<=" + algParameters.hotNum).drop(TEMP_RANK)

    modelOutput.hotVideoData = hotOrderedDF
  }

  //清理阶段
  override def afterInvoke(): Unit ={

  }
}





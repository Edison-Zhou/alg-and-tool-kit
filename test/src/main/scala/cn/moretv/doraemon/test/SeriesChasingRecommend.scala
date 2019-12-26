package cn.moretv.doraemon.test

import cn.moretv.doraemon.algorithm.seriesChasing.{SeriesChasingAlgorithm, SeriesChasingModel}
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.path.{DateRange, HdfsPath}
import cn.moretv.doraemon.test.constant.PathConstants
import cn.whaley.sdk.utils.TransformUDF
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
/**
  *
  *
  *  此类过时，将使用cn.moretv.doraemon.test.als.SeriesChasingRecommend类代替此类。
  *  此类只是移动目录
  * Created by cheng_huan on 2018/7/10.
  */
object SeriesChasingRecommend {

  def main(args: Array[String]): Unit = {
    try {
      val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
      val sc: SparkContext = ss.sparkContext
      import ss.implicits._
      /**
        * DataFrame[(uid, sid, optime, eposideIndex)]
        *
        */
      //读取HDFS数据
      val numDaysOfData = new DateRange("yyyyMMdd",-15)
      val scoreDataPath = new HdfsPath(numDaysOfData, PathConstants.pathOfMoretvSeriesScore)
      val scoreData = DataReader.read(scoreDataPath).rdd
        .map(r => (r.getLong(0), r.getString(1).toInt,
          r.getString(2), r.getString(3), r.getInt(4), r.getString(5).toInt, r.getDouble(6)))
        .toDF("userid", "sid", "optime", "content_type", "episodesid", "episode_index", "score")

      val sql4videoData = "select parent_sid, episode_index, create_time, content_type from tmp " +
        "where video_type = 2 " +
        "and episode_index is not null " +
        "and status = 1 " +
        "and type = 1 " +
        "and parent_sid is not null " +
        "and create_time is not null"
      val videoDataPath = new HdfsPath(PathConstants.pathOfMoretvProgram, sql4videoData)
      val videoData = DataReader.read(videoDataPath).rdd
        .map(r => (TransformUDF.transferSid(r.getString(0)), r.getInt(1), r.getTimestamp(2), r.getString(3)))
        .toDF("parent_sid", "episode_index", "create_time", "content_type")

      scoreData.printSchema()
      scoreData.show(10, false)

      videoData.printSchema()
      videoData.show(10, false)

      val seriesChasingAlg = new SeriesChasingAlgorithm()
      val seriesChasingDataMap = Map(seriesChasingAlg.INPUT_SCOREDATA_KEY -> scoreData, seriesChasingAlg.INPUT_VIDEODATA_KEY -> videoData)
      seriesChasingAlg.initInputData(seriesChasingDataMap)
      seriesChasingAlg.run()

      //得到模型结果
      val seriesChasingRecommend = seriesChasingAlg.getOutputModel.asInstanceOf[SeriesChasingModel]
      val seriesChasingRecommendPath = new HdfsPath("/ai/data/dw/moretv_tmp/seriesChasing")
//      seriesChasingRecommend.saveModel(seriesChasingRecommendPath)

    }catch {
      case e:Exception => throw e
    }
  }
}

package cn.moretv.doraemon.test

import java.text.SimpleDateFormat
import java.util.Date

import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.{FormatTypeEnum, ProductLineEnum}
import cn.moretv.doraemon.common.path.{HdfsPath, RedisPath}
import cn.moretv.doraemon.data.writer.{DataPack, DataPackParam, DataWriter2Kafka}
import cn.moretv.doraemon.test.util.BizUtils
import org.apache.spark.sql.functions._

/**
  * Created by lituo on 2018/8/13.
  */
object DetailPageRecommend extends BaseClass {
  implicit val productLine: ProductLineEnum.Value = ProductLineEnum.medusa

  override def execute(): Unit = {
    val contentTypeList = List("movie", "tv", "zongyi", "comic", "kids", "jilu")

    contentTypeList.foreach(contentType => {

      val similarDf = DataReader.read(new HdfsPath("/ai/output/medusa/similarMix/" + contentType + "/Latest"))
      val themeDf = DataReader.read(new HdfsPath("/ai/output/medusa/theme/" + contentType + "/Latest"))

      val similarFoldDf = similarDf.groupBy("sid").agg(collect_list("item")).toDF("sid","id").selectExpr("cast(sid as string) as sid", "id")
      val themeFoldDf = themeDf.toDF("sid", "id").selectExpr("cast(sid as string) as sid", "id")

      val dataPackParam = new DataPackParam
      dataPackParam.format = FormatTypeEnum.KV
      dataPackParam.extraValueMap = Map("alg" -> "mix")
      val similarPackDf = DataPack.pack(similarFoldDf, dataPackParam).toDF("key", "similar")

      val dataPackParam2 = new DataPackParam
      dataPackParam2.format = FormatTypeEnum.KV
      dataPackParam2.extraValueMap = Map("alg" -> "random", "title" -> "更多精彩")
      val themePackDf = DataPack.pack(themeFoldDf, dataPackParam2).toDF("key", "theme")

      val dataPackParam3 = new DataPackParam
      dataPackParam3.format = FormatTypeEnum.KV
      dataPackParam3.extraValueMap = Map("date" -> new SimpleDateFormat("yyyyMMdd").format(new Date()))
      val joinDf = similarPackDf.as("a").join(themePackDf.as("b"), expr("a.key = b.key"), "full")
        .selectExpr("case when a.key is not null then a.key else b.key end as key", "a.similar", "b.theme")

      val result = DataPack.pack(joinDf, dataPackParam3)
      result.show()

      val dataWriter = new DataWriter2Kafka
      val topic = "redis-moretv-topic-test"
      val host = "bigtest-cmpt-129-204"
      val port = 6379
      val dbIndex = 2
      val ttl = 8640000
      val formatType = FormatTypeEnum.KV

      val path = RedisPath(topic, host, port, dbIndex, ttl, formatType)
      dataWriter.write(result, path)



    })

  }
}

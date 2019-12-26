package cn.moretv.doraemon.data.writer

import cn.moretv.doraemon.common.enum.FormatTypeEnum
import cn.moretv.doraemon.common.util.DateUtils
import cn.moretv.doraemon.data.writer.DataPack.pack
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.junit.Test

/**
  * Created by lituo on 2018/6/27.
  */
class DataPackTest {
  val spark: SparkSession = SparkSession.builder().config(new SparkConf().setMaster("local")).getOrCreate()

  @Test
  def test(): Unit = {
    import spark.sqlContext.implicits._

    val df1 = Seq(
      ("a", Array("b", "bb"), 0.1),
      ("a", Array("c", "cc"), 0.2)
    ).toDF("u", "s", "score")

    val df2 = Seq(
      ("a", "[\"b\",\"bb\"]", "0.1"),
      ("a", "[\"c\",\"cc\"]", "0.2")
    ).toDF("u", "s", "score")


    val df3 = Seq(
      (123, Array((234, 0.1), (675, 0.2))),
      (456, Array((123, 0.1), (863, 0.2), (731, 0.3)))
    ).toDF("u", "s")

    val param = new DataPackParam
    param.format = FormatTypeEnum.ZSET
    param.zsetAlg = "mix"
    DataPack.pack(df3, param).show(false)

  }


  /**for 站点树重排序用户对应用户组数据的测试*/
  @Test
  def test2(): Unit = {
    import spark.sqlContext.implicits._

    val df1 = Seq(
      (2, "22"),
      (3, "33")
    ).toDF("uid", "groupId")

    val param = new DataPackParam
    param.format = FormatTypeEnum.KV
    param.keyPrefix = "o:msru:"
    DataPack.pack(df1, param).show(false)

  }

  @Test
  def test3(): Unit = {
    import spark.sqlContext.implicits._

    val portalDefaultDF = List(2).map(e => ("default", e)).toDF("uid", "sid")
    val vipDefaultDF = List(2,3,4).map(e => ("default", e)).toDF("uid", "sid")
    val hotDefaultDF = List("h1", "h2").map(e => ("default", e)).toDF("uid", "sid")

    portalDefaultDF.show(false)

//    val portalFoldDF = portalDefaultDF.groupBy("uid").agg(collect_list("sid")).toDF("uid", "id").selectExpr("cast(uid as string) as uid", "id")
    val portalFoldDF = portalDefaultDF
    val vipFoldDF = vipDefaultDF.groupBy("uid").agg(collect_list("sid")).toDF("uid", "id").selectExpr("cast(uid as string) as uid", "id")
    val hotFoldDF = hotDefaultDF.groupBy("uid").agg(collect_list("sid")).toDF("uid", "id").selectExpr("cast(uid as string) as uid", "id")

    portalFoldDF.show(false)

    val dataPackParam1 = new DataPackParam
    dataPackParam1.format = FormatTypeEnum.KV
    dataPackParam1.extraValueMap = Map("alg" -> "editor")
    val portalPackDF = DataPack.pack(portalFoldDF, dataPackParam1).toDF("key", "portal")

    portalPackDF.show(false)

    val dataPackParam2 = new DataPackParam
    dataPackParam2.format = FormatTypeEnum.KV
    dataPackParam2.extraValueMap = Map("alg" -> "random")
    val vipPackDF = DataPack.pack(vipFoldDF, dataPackParam2).toDF("key", "vip")

    vipPackDF.show(false)

    val dataPackParam3 = new DataPackParam
    dataPackParam3.format = FormatTypeEnum.KV
    dataPackParam3.extraValueMap = Map("alg" -> "hotRanking")
    val hotPackDF = DataPack.pack(hotFoldDF, dataPackParam3).toDF("key", "hot")

    hotPackDF.show(false)

    val dataPackParam4 = new DataPackParam
    dataPackParam4.format = FormatTypeEnum.KV
    dataPackParam4.keyPrefix = "p:a:"
    dataPackParam4.extraValueMap = Map("date" -> DateUtils.getTimeStamp)
    val unionDF = portalPackDF.as("a").join(vipPackDF.as("b"), expr("a.key = b.key"), "full").join(hotPackDF.as("c"), expr("a.key = c.key"), "full")
      .selectExpr("case when a.key is not null then a.key when b.key is not null then b.key else c.key end as key", "a.portal as portal", "b.vip as vip", "c.hot as hot")

    val frontPageUnionPackDF = DataPack.pack(unionDF, dataPackParam4)

    frontPageUnionPackDF.show(false)
  }

  def frontPageUnionPack(portalDF:DataFrame,
                         portalAlg:String,
                         vipDF:DataFrame,
                         vipAlg:String,
                         hotDF:DataFrame,
                         hotAlg:String):DataFrame = {
    val portalFoldDF = portalDF.groupBy("uid").agg(collect_list("sid")).toDF("uid", "id").selectExpr("cast(uid as string) as uid", "id")
    val vipFoldDF = vipDF.groupBy("uid").agg(collect_list("sid")).toDF("uid", "id").selectExpr("cast(uid as string) as uid", "id")
    val hotFoldDF = hotDF.groupBy("uid").agg(collect_list("sid")).toDF("uid", "id").selectExpr("cast(uid as string) as uid", "id")

    val dataPackParam1 = new DataPackParam
    dataPackParam1.format = FormatTypeEnum.KV
    dataPackParam1.extraValueMap = Map("alg" -> portalAlg)
    val portalPackDF = DataPack.pack(portalFoldDF, dataPackParam1).toDF("key", "portal")

    portalPackDF.show()

    val dataPackParam2 = new DataPackParam
    dataPackParam2.format = FormatTypeEnum.KV
    dataPackParam2.extraValueMap = Map("alg" -> vipAlg)
    val vipPackDF = DataPack.pack(vipFoldDF, dataPackParam2).toDF("key", "vip")

    vipPackDF.show(false)

    val dataPackParam3 = new DataPackParam
    dataPackParam3.format = FormatTypeEnum.KV
    dataPackParam3.extraValueMap = Map("alg" -> hotAlg)
    val hotPackDF = DataPack.pack(hotFoldDF, dataPackParam3).toDF("key", "hot")

    hotPackDF.show(false)

    val dataPackParam4 = new DataPackParam
    dataPackParam4.format = FormatTypeEnum.KV
    dataPackParam4.keyPrefix = "p:a:"
    dataPackParam4.extraValueMap = Map("date" -> DateUtils.getTimeStamp)
    val unionDF = portalPackDF.as("a").join(vipPackDF.as("b"), expr("a.key = b.key"), "full").join(hotPackDF.as("c"), expr("a.key = c.key"), "full")
      .selectExpr("case when a.key is not null then a.key when b.key is not null then b.key else c.key end as key", "a.portal as portal", "b.vip as vip", "c.hot as hot")

    DataPack.pack(unionDF, dataPackParam4)
  }
}

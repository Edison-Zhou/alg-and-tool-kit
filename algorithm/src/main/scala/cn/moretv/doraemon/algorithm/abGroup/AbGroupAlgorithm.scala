package cn.moretv.doraemon.algorithm.abGroup

import cn.moretv.doraemon.common.alg.Algorithm
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.json.JSONObject

/**
  * Created by cheng_huan on 2018/6/20.
  */
class AbGroupAlgorithm extends Algorithm{

  //数据Map的Key定义
  val INPUT_DATA_KEY = "input"
  //输入输出属性定义
  val algParameters: AbGroupParameters = new AbGroupParameters()
  val modelOutput: AbGroupModel = new AbGroupModel()
  //内部数据传递
  private val userColName: String = "uid"
  private val itemColName: String = "sid"
  private val algColName: String = "alg"

  /**
    * 用户AB分组
    * @param uid 用户userid
    * @param biz 业务名
    * @return alg
    */
  private def getUserABGroup(uid: Long, biz: String) = {
    val url = algParameters.urlHead  + uid.toString + algParameters.urlTail
    val json = new JSONObject(scala.io.Source.fromURL(url).mkString)
    val alg = json.getJSONObject(algParameters.groupJsonObjectName)
      .getJSONObject(biz)
      .getString(algParameters.groupJsonStringName)

    alg
  }

  /**
    * 视频AB分组
    * @param sid 视频sid
    * @param biz 业务名
    * @param contentType 视频类型
    * @return alg
    */
 private def getItemABGroup(sid: String, biz: String, contentType: String) = {
    val url = algParameters.urlHead  + sid + algParameters.urlTail
    val json = new JSONObject(scala.io.Source.fromURL(url).mkString)
    val alg = json.getJSONObject(algParameters.groupJsonObjectName)
      .getJSONObject(s"${biz}_$contentType")
      .getString(algParameters.groupJsonStringName)

    alg
  }

  //进行参数初始化设置
  override def beforeInvoke(): Unit =  {
    dataInput.get(INPUT_DATA_KEY) match {
      case None => throw new IllegalArgumentException("未设置输入数据")
      case _ =>
    }
  }

  //算子核心处理逻辑
  override def invoke(): Unit ={
    val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    import ss.implicits._

    val inputData = dataInput(INPUT_DATA_KEY)
    modelOutput.groupedData = algParameters.groupType match {
      case `userColName` => {
        inputData.rdd.map(e => {
          val uid = e.getLong(0)
          (uid, getUserABGroup(uid, algParameters.biz))
        }).toDF(userColName, algColName)
      }
      case `itemColName` => {
        inputData.rdd.map(e => {
          val sid = e.getString(0)
          (sid, getItemABGroup(sid, algParameters.biz, algParameters.contentType))
        }).toDF(itemColName, algColName)
      }
    }
  }

  //清理阶段
  override def afterInvoke(): Unit ={

  }
}

package cn.moretv.doraemon.algorithm.reorder2Replace

import cn.moretv.doraemon.common.alg.Algorithm
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.{collect_list, concat_ws, expr}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by guohao on 2019/1/7.
  * source:[sid,item,score]
  * replaceMap [sid,score]
  * 目标：替换原source结果中的item，并按照score排序
  * 1：如果replaceMap 中sid 在item 中，score来源于source
  *   如果replaceMap 中sid 不在item 中，score来源于replaceMap
  * 2:按照score降序返回
  */
class Reorder2ReplaceAlgorithm extends Algorithm{

  //输入数据Map的Key定义
  val INPUT_SOURCE = "source"

  //输入输出属性定义
  override protected val algParameters: Reorder2ReplaceParameters = new Reorder2ReplaceParameters()
  override protected val modelOutput: Reorder2ReplaceModel = new Reorder2ReplaceModel()

  override protected def beforeInvoke(): Unit = {
    dataInput.get(INPUT_SOURCE) match {
      case None => throw new IllegalArgumentException("未设置输入数据")
      case _ =>
    }

    algParameters.replaceSid.isEmpty match {
      case true => throw new IllegalArgumentException("replaceSid 未赋值")
      case _ =>
    }
  }

  override protected def invoke(): Unit = {

    val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    val sourceDf = dataInput(INPUT_SOURCE)
      .select(expr("sid"),concat_ws("_",expr("item"),expr("score")).as("itemScore"))
      .groupBy(expr("sid")).agg(collect_list(expr("itemScore")))

    val replaceSidBc = ss.sparkContext.broadcast(algParameters.replaceSid)

    val output =  getReorder(ss,replaceSidBc,sourceDf)

    modelOutput.reorder2ReplaceData  = output

  }

  override protected def afterInvoke(): Unit = {

  }

  def getReorder(ss:SparkSession,bc:Broadcast[Map[String,Double]], df:DataFrame): DataFrame ={
    import ss.implicits._
    //替换目标 sid,score
    val replaceSid = bc.value
    df.rdd.flatMap(row=>{
      val sid = row.getAs[String](0)
      val itemScore = row.getSeq[String](1)
      var map : Map[String,Double] = Map()
      itemScore.foreach(f=>{
        val item =  f.split("_")(0)
        val similarity =  f.split("_")(1).toDouble
        map += (item->similarity)
      })

      //节目,替换节目,score
      replaceSid.keys.toList.map(key=>{
        val score = if( map.get(key).isEmpty){
          //如果替换节目sid未在源sid中，取替换节目sid的评分
          replaceSid.get(key).get
        }else{
          //如果替换节目sid在源sid中，取原节目sid的评分get
          map.get(key).get
        }
        (sid,key,score)
      })

    }).toDF("sid","item","score")
//      .sort(expr("sid"),expr("score").desc)
//      .groupBy("sid").agg(collect_list(expr("item")).as("item"))
  }
}

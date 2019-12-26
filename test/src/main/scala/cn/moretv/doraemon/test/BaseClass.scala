package cn.moretv.doraemon.test

import cn.moretv.doraemon.common.enum.EnvEnum
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * Created by lituo on 2018/6/13.
  */
trait BaseClass {


   //.setMaster("local") 是本地运行
  val config = new SparkConf().setMaster("local[2]")

  //服务器运行
//  val config = new SparkConf()

  //本地测试需要指定，服务器中已经定义
//  config.set("spark.sql.crossJoin.enabled", "true")
  /**
    * define some parameters
    */
  implicit var spark: SparkSession = null
  var sc: SparkContext = null
  implicit var sqlContext: SQLContext = null

  implicit val env = EnvEnum.TEST


  /**
    * 程序入口
    *
    * @param args
    */
  def main(args: Array[String]) {
    System.out.println("init start ....")
    init()
    System.out.println("init success ....")

    execute()


  }

  /**
    * 全局变量初始化
    */
  def init(): Unit = {
    spark = SparkSession.builder()
      .config(config)
      .getOrCreate()
    sc = spark.sparkContext
    sqlContext = spark.sqlContext
  }

  def execute(): Unit
}

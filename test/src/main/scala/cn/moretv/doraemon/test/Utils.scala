package cn.moretv.doraemon.test

import org.apache.spark.rdd.RDD
import org.json.JSONObject
import scala.collection.mutable.ArrayBuffer

/**
  * Created by cheng_huan on 2017/12/13.
  */
object Utils {
  /**
    * 电影分组
    * @param uid 用户userid
    * @return 用户分组标示
    */
  def getUserABGroup(uid: Long) = {
    val url = s"http://10.19.140.137:3456/config/abTest?userId=$uid&version=moretv"
    val json = new JSONObject(scala.io.Source.fromURL(url).mkString)
    val alg = json.getJSONObject("abTest")
      .getJSONObject("portalRecommend")
      .getString("alg")

    alg
  }

  /**
    * 将两个数组按一定频率穿插组合起来
    * @param a 数组1
    * @param b 数组2
    * @param f1 取数组1元素的频率
    * @param f2 去数组2元素的频率
    * @return 组合后的数组
    */
  def arrayAlternate(a:Array[Int], b:Array[Int], f1:Int, f2:Int) : Array[Int] = {
    val result = new ArrayBuffer[Int]()
    var index_1 = 0
    var index_2 = 0

    if(a.length > 0 && b.length > 0) {
      while(index_1 < a.length || index_2 < b.length)
      {
        for(i <- index_1 until index_1 + f1)
        {
          if(i < a.length) result += a(i)
        }
        index_1 = index_1 + f1


        for(j <- index_2 until index_2 + f2)
        {
          if(j < b.length) result += b(j)
        }
        index_2 = index_2 + f2
      }

      result.toArray
    }else{
      if(a.length == 0) {
        b
      }else{
        a
      }
    }
  }

  /**
    * 通过从初始数组中随机选出num个，将数组分为两部分
    * @param originalArray 初始数组
    * @param num 随机选出的个数
    * @return （随机选出的num个元素， 剩余的元素）
    */
  def randomTake(originalArray:Array[Int], num:Int): Array[Int] = {
    randomArray(originalArray).take(num)
  }

  /**
    * 从一个数组中选取前num个元素，然后顺序随机化
    * @param originalArray 初始数组
    * @param num 数量
    * @return 选出并随机的数组
    */
  def takeThenRandom(originalArray:Array[Int], num:Int): Array[Int] = {
    randomArray(originalArray.take(num))
  }

  /**
    * 将一个Array随机化
    * @param sidArray 列表
    * @return 随机化后的sidArray
    */
  def randomArray(sidArray: Array[Int]): Array[Int] = {

    val transferArray = new ArrayBuffer[Int]()
    val randomArray = new ArrayBuffer[Int]()

    for(i <- 0 until sidArray.length) transferArray += sidArray(i)

    while (transferArray.length > 0) {
      val randomIndex = (math.random * transferArray.length).toInt

      randomArray += transferArray(randomIndex)
      transferArray.remove(randomIndex)
    }

    randomArray.toArray
  }

  /**
    * 对用户原始数据进行整合
    * @param rawData rawData RDD[(uid, sid, latest_optime, episodeIndex)]
    * @param latestActive RDD[(uid, latestActive_time)]
    * @return RDD[(uid, latestActive_time, Array[(sid, latest_opTime, latest_episodeIndex, penultimate_episodeIndex)])]
    */
  def userBehaviorIntegrate(rawData: RDD[(Long, Int, String, Int)], latestActive: RDD[(Long, String)]):
  RDD[(Long, String, Array[(Int, String, Int, Int)])] = {
    val result = rawData.map(e => ((e._1, e._2), (e._3, e._4)))
      .groupByKey()
      .map(e => (e._1, e._2.toArray.sortBy(x => x._1).takeRight(2)))
      .filter(e => e._2.length > 0)
      .map(e => {
        val uid = e._1._1
        val sid = e._1._2
        val watches = e._2

        var latest_opTime = ""
        var latest_episodeIndex = -1
        var penultimate_episodeIndex = 0

        if(watches.length == 2) {
          latest_opTime = watches(1)._1
          latest_episodeIndex = watches(1)._2
          penultimate_episodeIndex = watches(0)._2
        } else {
          latest_opTime = watches(0)._1
          latest_episodeIndex = watches(0)._2
        }

        (uid, (sid, latest_opTime, latest_episodeIndex, penultimate_episodeIndex))
      })
      .groupByKey()
      .map(e => (e._1, e._2.toArray))

    result.join(latestActive)
      .map(e => (e._1, e._2._2, e._2._1))
  }


}

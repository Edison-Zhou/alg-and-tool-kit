package cn.moretv.doraemon.common.util

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
/**
  *
  * @author wang.baozhi 
  * @since 2018/7/27 下午12:33 
  */
object ArrayUtils {
  /**
    * 将两个数组按一定频率穿插组合起来
    *
    * @param a  数组1
    * @param b  数组2
    * @param f1 取数组1元素的频率
    * @param f2 去数组2元素的频率
    * @return 组合后的数组
    */
  def arrayAlternate[T:ClassTag](a: Array[T], b: Array[T], f1: Int, f2: Int): Array[T] = {
    val result = new ArrayBuffer[T]()
    var index_1 = 0
    var index_2 = 0

    if (a.length > 0 && b.length > 0) {
      while (index_1 < a.length || index_2 < b.length) {
        for (i <- index_1 until index_1 + f1) {
          if (i < a.length) result += a(i)
        }
        index_1 = index_1 + f1


        for (j <- index_2 until index_2 + f2) {
          if (j < b.length) result += b(j)
        }
        index_2 = index_2 + f2
      }

      result.toArray
    } else {
      if (a.length == 0) {
        b
      } else {
        a
      }
    }
  }


  /**
    * 将一个Array随机化
    * @param sidArray 列表
    * @return 随机化后的sidArray
    */
  def randomArray[T:ClassTag](sidArray: Array[T]): Array[T] = {

    val transferArray = new ArrayBuffer[T]()
    val randomArray = new ArrayBuffer[T]()

    for(i <- 0 until sidArray.length) transferArray += sidArray(i)

    while (transferArray.length > 0) {
      val randomIndex = (math.random * transferArray.length).toInt

      randomArray += transferArray(randomIndex)
      transferArray.remove(randomIndex)
    }

    randomArray.toArray[T]
  }

  /**
    * 从一个数组中选取前num个元素，然后顺序随机化
    * @param originalArray 初始数组
    * @param num 数量
    * @return 选出并随机的数组
    */
  def takeThenRandom[T:ClassTag](originalArray:Array[T], num:Int): Array[T] = {
    randomArray(originalArray.take(num))
  }


  /**
    * 通过从初始数组中随机选出num个，将数组分为两部分
    * @param originalArray 初始数组
    * @param num 随机选出的个数
    * @return （随机选出的num个元素， 剩余的元素）
    */
  def randomTake[T:ClassTag](originalArray:Array[T], num:Int): Array[T] = {
    randomArray(originalArray).take(num)
  }

}

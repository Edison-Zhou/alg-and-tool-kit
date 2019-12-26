package cn.moretv.doraemon.algorithm.randomCluster

import cn.moretv.doraemon.common.alg.Model
import org.apache.spark.sql._

/**
  * Created by cheng_huan on 2018/8/13.
  */
class RandomClusterModel extends Model{
  val modelName: String = "RandomCluster"

  var randomClusterData:DataFrame = null
}

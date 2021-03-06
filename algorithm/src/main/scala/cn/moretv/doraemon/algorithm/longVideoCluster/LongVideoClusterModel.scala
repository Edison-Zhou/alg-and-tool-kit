package cn.moretv.doraemon.algorithm.longVideoCluster

import cn.moretv.doraemon.common.alg.Model
import org.apache.spark.sql._

/**
  *
  * @author wang.baozhi 
  * @since 2018/7/26 下午5:26 
  */
class LongVideoClusterModel extends Model {
  override val modelName: String = "LongVideoClusterModel"
  var longVideoClusterDataFrame: DataFrame = null
  val uidColName = "uid"
  val sidColName = "sid"

}

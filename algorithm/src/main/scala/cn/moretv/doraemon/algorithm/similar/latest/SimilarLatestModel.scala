package cn.moretv.doraemon.algorithm.similar.latest

import cn.moretv.doraemon.common.alg.Model
import org.apache.spark.sql._

/**
  *
  * @author wang.baozhi 
  * @since 2018/7/30 上午11:17 
  */
class SimilarLatestModel extends Model {
  override val modelName: String = "SimilarLatestModel"
  var similarLatestModel: DataFrame = null
  val uidColName = "uid"
  val sidColName = "sid"

}

package cn.moretv.doraemon.algorithm.similar.last4offlineindicator

import cn.moretv.doraemon.common.alg.Model
import org.apache.spark.sql._

/**
  *
  * @author wang.baozhi 
  * @since 2018/7/30 上午11:17 
  */
class Last4OfflineIndicatorModel extends Model {
  override val modelName: String = "Last4OfflineIndicatorModel"
  var similarLatestModel: DataFrame = null
  val uidColName = "uid"
  val sidColName = "sid"

}

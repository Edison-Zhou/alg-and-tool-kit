package cn.moretv.doraemon.algorithm.abGroup

import cn.moretv.doraemon.common.alg.Model
import org.apache.spark.sql.DataFrame

/**
  * Created by cheng_huan on 2018/6/20.
  */
class AbGroupModel extends Model {
  val modelName: String = "AbGroup"
  var groupedData: DataFrame = null

}

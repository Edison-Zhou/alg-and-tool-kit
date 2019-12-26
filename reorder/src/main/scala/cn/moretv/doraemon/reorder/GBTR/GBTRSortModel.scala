package cn.moretv.doraemon.reorder.GBTR

import cn.moretv.doraemon.common.alg.Model
import org.apache.spark.sql._

/**
  * Created by cheng_huan on 2019/7/3.
  */
class GBTRSortModel extends Model{
  override val modelName: String = "GBTRSort"
  var reorderedDataFrame: DataFrame = null
}

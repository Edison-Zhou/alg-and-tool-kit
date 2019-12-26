package cn.moretv.doraemon.reorder.linearRegression

import cn.moretv.doraemon.common.alg.Model
import org.apache.spark.sql.DataFrame

/**
  * Created by cheng_huan on 2019/7/2.
  */
class LinearRegressionSortModel extends Model{
  override val modelName: String = "LinearRegressionSort"
  var reorderedDataFrame: DataFrame = null
}

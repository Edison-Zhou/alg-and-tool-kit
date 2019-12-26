package cn.moretv.doraemon.algorithm.seriesChasing

import cn.moretv.doraemon.common.alg.Model
import org.apache.spark.sql._

/**
  * Created by cheng_huan on 2018/6/26.
  */
class SeriesChasingModel extends Model {
  override val modelName: String = "SeriesChasingModel"
  var seriesChasingData: DataFrame = null

}

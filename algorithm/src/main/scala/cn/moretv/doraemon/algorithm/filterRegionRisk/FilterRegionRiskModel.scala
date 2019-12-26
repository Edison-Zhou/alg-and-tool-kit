package cn.moretv.doraemon.algorithm.filterRegionRisk

import cn.moretv.doraemon.common.alg.Model
import cn.moretv.doraemon.common.path.HdfsPath
import cn.moretv.doraemon.common.util.HdfsUtil
import org.apache.spark.sql._

/**
  * Created by cheng_huan on 2018/6/25.
  */
class FilterRegionRiskModel extends Model{
  override val modelName: String = "FilterRegionRiskModel"
  var filteredData: DataFrame = null

}

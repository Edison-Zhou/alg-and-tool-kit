package cn.moretv.doraemon.algorithm.validationCheck

import cn.moretv.doraemon.common.alg.Model
import cn.moretv.doraemon.common.path.HdfsPath
import cn.moretv.doraemon.common.util.HdfsUtil
import org.apache.spark.sql.DataFrame

/**
  * Created by cheng_huan on 2018/6/22.
  */
class ValidationCheckModel extends Model{
  override val modelName: String = "ValidationCheckModel"
  var checkedData: DataFrame = null

}

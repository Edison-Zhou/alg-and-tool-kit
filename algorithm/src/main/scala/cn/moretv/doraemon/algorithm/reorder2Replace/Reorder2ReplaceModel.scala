package cn.moretv.doraemon.algorithm.reorder2Replace

import cn.moretv.doraemon.common.alg.Model
import org.apache.spark.sql.DataFrame

/**
  * Created by guohao on 2019/1/7.
  */
class Reorder2ReplaceModel extends Model{
  /**
    * 模型名称
    */
  override val modelName: String = "Reorder2Replace"
  var reorder2ReplaceData:DataFrame = null
}

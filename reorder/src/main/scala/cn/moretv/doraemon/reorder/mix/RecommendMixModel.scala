package cn.moretv.doraemon.reorder.mix

import cn.moretv.doraemon.common.alg.Model
import org.apache.spark.sql.DataFrame

/**
  * Created by lituo on 2018/6/20.
  */
class RecommendMixModel extends Model {
  /**
    * 模型名称
    */
  override val modelName = "mix"

  var mixResult: DataFrame = _

}

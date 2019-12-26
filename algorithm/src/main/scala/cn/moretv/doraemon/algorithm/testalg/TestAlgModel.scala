package cn.moretv.doraemon.algorithm.testalg

import cn.moretv.doraemon.common.alg.Model
import org.apache.spark.sql.DataFrame

/**
  * Created by lituo on 2018/6/13.
  */
class TestAlgModel extends Model {
  /**
    * 模型名称
    */
  override val modelName = "test_alg"

  var data: DataFrame = _

}

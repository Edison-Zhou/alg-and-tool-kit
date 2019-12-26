package cn.moretv.doraemon.algorithm.matrix.unfold

import cn.moretv.doraemon.common.alg.Model
import org.apache.spark.sql.DataFrame

/**
  * Created by baozhiwang on 2018/6/21.
  */
class MatrixUnFoldModel extends Model {
  val modelName: String = "matrixUnFold"

  var matrixUnFold: DataFrame = null
}

package cn.moretv.doraemon.algorithm.matrix.fold

import cn.moretv.doraemon.common.alg.Model
import org.apache.spark.sql.DataFrame

/**
  * Created by baozhiwang on 2018/6/21.
  */
class MatrixFoldModel extends Model {
  val modelName: String = "matrixFold"

  var matrixFold: DataFrame = null

}

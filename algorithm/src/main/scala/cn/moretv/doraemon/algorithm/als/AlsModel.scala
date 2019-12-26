package cn.moretv.doraemon.algorithm.als

import cn.moretv.doraemon.common.alg.Model
import org.apache.spark.sql.DataFrame

class AlsModel extends Model {
  val modelName: String = "ALS"

  //U矩阵
  var matrixU: DataFrame = null
  //V矩阵
  var matrixV: DataFrame = null

  val alsUerColName = "user"
  val alsItemColName = "item"

}

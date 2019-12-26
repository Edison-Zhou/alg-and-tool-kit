package cn.moretv.doraemon.algorithm.similar.cluster

import cn.moretv.doraemon.common.alg.Model
import org.apache.spark.sql.DataFrame

class SimilarClusterModel extends Model {
  val modelName: String = "SimilarCategory"


  //计算出来的相似节目的结果
  var matrixData: DataFrame = null

}

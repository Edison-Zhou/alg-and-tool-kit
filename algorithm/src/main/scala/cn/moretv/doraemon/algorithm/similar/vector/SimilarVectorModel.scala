package cn.moretv.doraemon.algorithm.similar.vector

import cn.moretv.doraemon.common.alg.Model
import org.apache.spark.sql.DataFrame

class SimilarVectorModel extends Model {
  val modelName: String = "SimilarVector"


  //计算出来的相似节目的结果
  var matrixData: DataFrame = null

}

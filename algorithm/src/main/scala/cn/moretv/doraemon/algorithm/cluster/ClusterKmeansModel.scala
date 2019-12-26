package cn.moretv.doraemon.algorithm.cluster

import cn.moretv.doraemon.common.alg.Model
import cn.moretv.doraemon.common.path.HdfsPath
import org.apache.spark.sql.DataFrame

class ClusterKmeansModel extends Model {
  val modelName: String = "ClusterKmeans"

  var matrixData:DataFrame = null

}

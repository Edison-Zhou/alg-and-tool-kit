package cn.moretv.doraemon.algorithm.kMeans

import cn.moretv.doraemon.common.alg.Model
import org.apache.spark.mllib.clustering.KMeansModel


/**
  *
  * @author wang.baozhi 
  * @since 2018/10/8 上午8:45 
  */
class KMModel extends Model {
  override val modelName: String = "kMeansModel"
  var kMeansModel: KMeansModel = null

}

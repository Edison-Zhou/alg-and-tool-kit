package cn.moretv.doraemon.algorithm.hotVideo

import cn.moretv.doraemon.common.alg.Model
import org.apache.spark.sql.DataFrame

class HotVideoModel extends Model {
  val modelName: String = "HotVideo"

  //热门影片
  var hotVideoData: DataFrame = _


}

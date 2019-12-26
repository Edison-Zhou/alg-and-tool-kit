package cn.moretv.doraemon.algorithm.relevantSubject

import cn.moretv.doraemon.common.alg.Model
import org.apache.spark.sql.DataFrame

/**
  * Created by cheng_huan on 2018/8/22.
  */
class RelevantSubjectModel extends Model{
  val modelName: String = "relevantSubject"
  var subjectData: DataFrame = null
}

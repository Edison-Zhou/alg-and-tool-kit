package cn.moretv.doraemon.reorder.logistic

import cn.moretv.doraemon.common.alg.Model
import org.apache.spark.sql.DataFrame


/**
  *
  * @author liu.qiang 
  * @since 2018/7/30 14:32
  *
  * 只需要利用logistic训练好的模型来做预测，不需要保存logistic-predict模型
  */
class LogisticPredictModel  extends Model {

  val modelName: String = "logistic-predict"
  var logisticPredictResult: DataFrame = _

//  /** 保存模型数据 */
  //  def saveModel(modelPath: HdfsPath):Boolean = {
  //    logisticPredictResult.select("user", "video", "predict").
  //      write.save(modelPath.getHdfsPath()+File.separator+this.modelName)
  //    true
  //  }
  //  /** 读取模型数据 */
  //  def loadModel(modelPath: HdfsPath)= true

}

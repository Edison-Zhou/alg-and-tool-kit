package cn.moretv.doraemon.reorder.logistic

import cn.moretv.doraemon.common.alg.AlgParameters
import cn.moretv.doraemon.common.path.HdfsPath

/**
  *
  * @author liu.qiang 
  * @since 2018/7/30 14:33 
  */
class LogisticPredictParameters extends AlgParameters{
  /**
    * 算法名称
    */
  override val algName = "logistic-predict"

  /**
    * 每个用户推荐的节目数量
    */
  var recommendNum: Int = 20

  /**
    * logistic回归模型输入的dataframe字段
    */
  var recommendUserColumn: String = "user"
  var recommendItemColumn: String = "item"
  var scoreColumn: String = "score"

  /**
    * logistic回归预测结果字段
    */
  var recommendResColumn: String = "recommends"

  /**
    * 通过解析json字符串的方式填充参数
    *
    * @param jsonString 参数key和内容组成的json字符创
    * @return
    */
  override def updateFromJsonString(jsonString: String) = false

  /**
    * 通过读取hdfs中的存储的内容填充参数
    *
    * @param hdfsPath
    * @return
    */
  override def loadFromHdfs(hdfsPath: HdfsPath) = false

  /**
    * 把设定好的参数存储到hdfs
    *
    * @param hdfsPath
    * @return
    */
  override def saveToHdfs(hdfsPath: HdfsPath) = false

  /**
    * 参数内容验证
    *
    * @return 如果成功返回"ok"或者空，失败返回错误信息
    */
  override def validation() = "ok"

}

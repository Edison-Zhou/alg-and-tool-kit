package cn.moretv.doraemon.reorder.logistic

import cn.moretv.doraemon.common.alg.AlgParameters
import cn.moretv.doraemon.common.path.HdfsPath

/**
  * Created by liu.qiang on 2018/6/28.
  */
class LogisticParameters extends AlgParameters{
  /**
    * 算法名称
    */
  override val algName = "logistic"

  object Alg  extends Enumeration{
    type Alg = Value
    val LR = Value
  }

  object RegType  extends Enumeration{
    type RegType = Value
    val L1, L2 = Value
  }

  import Alg._
  import RegType._

  case class Params(
                     input: String = null,
                     numIterations: Int = 100,
                     stepSize: Double = 1.0,
                     algorithm: Alg = LR,
                     regType: RegType = L2,
                     regParam: Double = 0.01)

  var params:Params = _

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
  override def validation(): String = {
    if(this.params.isInstanceOf[Params]) "ok" else "type error"
  }


}

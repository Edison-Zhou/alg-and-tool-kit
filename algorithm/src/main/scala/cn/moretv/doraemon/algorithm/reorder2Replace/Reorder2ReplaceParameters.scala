package cn.moretv.doraemon.algorithm.reorder2Replace

import cn.moretv.doraemon.common.alg.AlgParameters
import cn.moretv.doraemon.common.path.HdfsPath

/**
  * Created by guohao on 2019/1/7.
  */
class Reorder2ReplaceParameters extends AlgParameters{
  /**
    * 算法名称
    */
  override val algName: String = "reorder2Replace"


  /**
    * 替换的sid
    */
  var replaceSid:Map[String,Double] = Map()

  /**
    * 通过解析json字符串的方式填充参数
    *
    * @param jsonString 参数key和内容组成的json字符创
    * @return
    */
  override def updateFromJsonString(jsonString: String): Boolean = false

  /**
    * 通过读取hdfs中的存储的内容填充参数
    *
    * @param hdfsPath
    * @return
    */
override def loadFromHdfs(hdfsPath: HdfsPath): Boolean = false

  /**
    * 把设定好的参数存储到hdfs
    *
    * @param hdfsPath
    * @return
    */
  override def saveToHdfs(hdfsPath: HdfsPath): Boolean = false

  /**
    * 参数内容验证
    *
    * @return 如果成功返回"ok"或者空，失败返回错误信息
    */
  override def validation(): String = "ok"
}

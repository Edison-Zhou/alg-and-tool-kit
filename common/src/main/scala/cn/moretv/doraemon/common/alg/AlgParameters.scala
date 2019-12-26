package cn.moretv.doraemon.common.alg

import cn.moretv.doraemon.common.path.HdfsPath

/**
  * Created by lituo on 2018/6/12.
  */
trait AlgParameters extends Serializable{

  /**
    * 算法名称
    */
  val algName: String

  /**
    * 通过解析json字符串的方式填充参数
    * @param jsonString 参数key和内容组成的json字符创
    * @return
    */
  def updateFromJsonString(jsonString: String): Boolean

  /**
    * 通过读取hdfs中的存储的内容填充参数
    * @param hdfsPath
    * @return
    */
  def loadFromHdfs(hdfsPath: HdfsPath): Boolean

  /**
    * 把设定好的参数存储到hdfs
    * @param hdfsPath
    * @return
    */
  def saveToHdfs(hdfsPath: HdfsPath): Boolean

  /**
    * 参数内容验证
    * @return 如果成功返回"ok"或者空，失败返回错误信息
    */
  def validation(): String
}

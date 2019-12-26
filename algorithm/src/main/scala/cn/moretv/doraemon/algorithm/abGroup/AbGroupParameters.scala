package cn.moretv.doraemon.algorithm.abGroup

import cn.moretv.doraemon.common.alg.AlgParameters
import cn.moretv.doraemon.common.path.HdfsPath

/**
  * Created by cheng_huan on 2018/6/20.
  */
class AbGroupParameters extends AlgParameters{
  val algName: String = "AbGroup"
  var biz: String = null
  var groupType: String = null
  var contentType: String = null
  var urlHead: String = "http://10.19.140.137:3456/config/abTest?userId="
  var urlTail: String = "&version=moretv"
  var groupJsonObjectName: String = "abTest"
  var groupJsonStringName: String = "alg"

  def validation(): String ={
    var exception: String = ""
    var illegalParameters: Array[String] = Array.empty[String]
    if(biz == null) illegalParameters = illegalParameters :+ "biz"
    if(groupType == null) illegalParameters = illegalParameters :+ "groupType"
    if(groupType == "sid" && contentType == null) illegalParameters = illegalParameters :+ "contentType"
    if(illegalParameters.length > 1)
      exception = "parameters " + illegalParameters.mkString(",") + " are illegal"
    if(illegalParameters.length == 1)
      exception = "parameter " + illegalParameters.mkString(",") + " is illegal"
    exception
  }


  def updateFromJsonString(jsonString: String): Boolean = {
    false
  }

  def updateFromMap(paramMap: Map[String, String]): Boolean = {
    false
  }

  def loadFromHdfs(hdfsPath: HdfsPath): Boolean = {
    false
  }

  def saveToHdfs(hdfsPath: HdfsPath): Boolean = {
    false
  }
}

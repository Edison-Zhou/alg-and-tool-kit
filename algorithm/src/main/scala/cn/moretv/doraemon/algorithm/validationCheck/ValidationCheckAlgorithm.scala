package cn.moretv.doraemon.algorithm.validationCheck

import cn.moretv.doraemon.common.alg.Algorithm

/**
  * Created by cheng_huan on 2018/6/22.
  */
class ValidationCheckAlgorithm extends Algorithm {
  //数据Map的Key定义
  val INPUT_DATA_KEY = "input"
  val INPUT_CHECKLIST_KEY = "checklist"
  //输入输出属性定义
  val algParameters: ValidationCheckParameters = new ValidationCheckParameters()
  val modelOutput: ValidationCheckModel = new ValidationCheckModel()


  //进行参数初始化设置
  override def beforeInvoke(): Unit = {
    dataInput.get(INPUT_DATA_KEY) match {
      case None => throw new IllegalArgumentException("未设置输入数据")
      case _ =>
    }
    dataInput.get(INPUT_CHECKLIST_KEY) match {
      case None => throw new IllegalArgumentException("未设置输入数据")
      case _ =>
    }
  }

  override def invoke(): Unit = {
    val data = dataInput(INPUT_DATA_KEY)
    val checkList = dataInput(INPUT_CHECKLIST_KEY).toDF("condition")
    val whiteOrBlackList: String = algParameters.whiteOrBlackList
    val userOrItem: String = algParameters.userOrItem
    modelOutput.checkedData = whiteOrBlackList match {
      case "whiteList" => {
        userOrItem match {
          case "user" => {
            data.join(checkList, data("uid") === checkList("condition"), "left")
              .filter("condition is not null").drop("condition")
          }
          case "item" => {
            data.join(checkList, data("sid") === checkList("condition"), "left")
              .filter("condition is not null").drop("condition")
          }
          case _ => throw new IllegalArgumentException("不合法的userOrItem参数：" + userOrItem)
        }
      }
      case "blackList" => {
        userOrItem match {
          case "user" => {
            data.join(checkList, data("uid") === checkList("condition"), "left")
              .filter("condition is null").drop("condition")
          }
          case "item" => {
            data.join(checkList, data("sid") === checkList("condition"), "left")
              .filter("condition is null").drop("condition")
          }
          case _ => throw new IllegalArgumentException("不合法的userOrItem参数：" + userOrItem)
        }
      }
      case _ => throw new IllegalArgumentException("不合法的whiteOrBlackList参数：" + whiteOrBlackList)
    }
  }

  //清理阶段
  override def afterInvoke(): Unit = {

  }
}

package cn.moretv.doraemon.algorithm.filterRegionRisk

import cn.moretv.doraemon.common.alg.Algorithm

/**
  * Created by cheng_huan on 2018/6/25.
  */
class FilterRegionRiskAlgorithm extends Algorithm{
  //数据Map的Key定义
  val INPUT_DATA_KEY = "input"
  val INPUT_USERRISK_KEY = "userRisk"
  val INPUT_ITEMRISK_KEY = "itemRisk"
  //输入输出属性定义
  val algParameters: FilterRegionRiskParameters = new FilterRegionRiskParameters()
  val modelOutput: FilterRegionRiskModel = new FilterRegionRiskModel()

  //进行参数初始化设置
  override def beforeInvoke(): Unit =  {
    dataInput.get(INPUT_DATA_KEY) match {
      case None => throw new IllegalArgumentException("未设置输入数据")
      case _ =>
    }
    dataInput.get(INPUT_USERRISK_KEY) match {
      case None => throw new IllegalArgumentException("未设置输入数据")
      case _ =>
    }
    dataInput.get(INPUT_ITEMRISK_KEY) match {
      case None => throw new IllegalArgumentException("未设置输入数据")
      case _ =>
    }
  }

  override def invoke(): Unit = {
    val data = dataInput(INPUT_DATA_KEY).toDF("uid", "sid")
    val userRisk = dataInput(INPUT_USERRISK_KEY).toDF("uid", "userRisk")
    val itemRisk = dataInput(INPUT_ITEMRISK_KEY).toDF("sid", "videoRisk")
    modelOutput.filteredData = data.join(userRisk, data("uid") === userRisk("uid"), "left")
      .join(itemRisk, data("sid") === itemRisk("sid"), "left")
      .filter("userRisk is null or videoRisk is null")
      .filter("userRisk + videoRisk <= 2")
  }

  //清理阶段
  override def afterInvoke(): Unit ={

  }
}

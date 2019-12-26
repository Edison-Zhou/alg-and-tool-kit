package cn.moretv.doraemon.algorithm.testalg

import cn.moretv.doraemon.common.alg.Algorithm
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by lituo on 2018/6/15.
  * 测试算子：对数据表按指定列升序或倒序
  *
  */
class TestAlg extends Algorithm {

  override protected val algParameters: TestParameter = new TestParameter  //用子类类型
  override protected val modelOutput: TestAlgModel = new TestAlgModel  //用子类类型
  private var inputDataFrame: DataFrame = _

  val dataFrameKey = "data"

  override protected def beforeInvoke(): Unit = {
    //执行一些数据和参数的校验
    dataInput.get(dataFrameKey) match {
      case Some(dataFrame) => inputDataFrame = dataFrame
      case None => throw new IllegalArgumentException("未设置输入数据")
    }

  }

  override protected def invoke(): Unit = {
    //计算过程
    println(algParameters.desc)
    println(algParameters.columnName)
    val column = if (algParameters.desc) col(algParameters.columnName).desc else col(algParameters.columnName).asc
    //结果赋值到输出model
    modelOutput.data = inputDataFrame.orderBy(column)
  }

  override protected def afterInvoke(): Unit = {}
}

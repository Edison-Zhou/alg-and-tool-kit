package cn.moretv.doraemon.reorder.GBTR

import cn.moretv.doraemon.common.alg.{AlgParameters, Algorithm, Model}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.regression.GBTRegressionModel
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Created by cheng_huan on 2019/7/3.
  */
class GBTRSortAlgorithm extends Algorithm{
  override protected val algParameters: GBTRSortParameters = new GBTRSortParameters
  override protected val modelOutput: GBTRSortModel = new GBTRSortModel

  val INPUT_DATA = "data"

  override protected def beforeInvoke(): Unit = {
    dataInput.get(INPUT_DATA) match {
      case None => throw new IllegalArgumentException("未设置输入数据:召回数据")
      case _ =>
    }
  }

  override protected def invoke(): Unit = {
    val ss: SparkSession = SparkSession.builder().getOrCreate()
    import ss.implicits._

    val GBTRModel = GBTRegressionModel.load(algParameters.modelPath)
    require(GBTRModel.numFeatures == algParameters.featureSize, "NumFeatures Of GBTRModel is not equal to feature size.")

    val data = dataInput(INPUT_DATA)
    val result = GBTRModel.transform(data).select("uid", "sid", "prediction")
      .withColumn("tmp", row_number().over(Window.partitionBy("uid").orderBy(col("prediction").desc)))
      .filter(s"tmp <= ${algParameters.recommendSize}").drop("tmp")

    modelOutput.reorderedDataFrame = result
  }

  override protected def afterInvoke(): Unit = {}

}

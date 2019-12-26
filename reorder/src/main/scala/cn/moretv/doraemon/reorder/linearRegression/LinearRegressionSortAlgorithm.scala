package cn.moretv.doraemon.reorder.linearRegression

import cn.moretv.doraemon.common.alg.{AlgParameters, Algorithm, Model}
import org.apache.spark.SparkConf
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Created by cheng_huan on 2019/7/2.
  */
class LinearRegressionSortAlgorithm extends Algorithm{
  override protected val algParameters: LinearRegressionSortParameters = new LinearRegressionSortParameters()

  override protected val modelOutput: LinearRegressionSortModel = new LinearRegressionSortModel()

  //数据Map的Key定义
  //召回的待排序节目
  val INPUT_DATA = "data"

  override protected def beforeInvoke(): Unit = {
    dataInput.get(INPUT_DATA) match {
      case None => throw new IllegalArgumentException("未设置输入数据:召回数据")
      case _ =>
    }
  }

  override protected def invoke(): Unit = {
    val ss = SparkSession.builder().getOrCreate()
    import ss.implicits._

    val LRModel = LinearRegressionModel.load(algParameters.modelPath)

    require(LRModel.numFeatures == algParameters.featureSize, "NumFeatures Of LRModel is not equal to feature size.")

    val data = dataInput(INPUT_DATA)

    val result = LRModel.transform(data).select("uid", "sid", "prediction")
      .withColumn("tmp", row_number().over(Window.partitionBy("uid").orderBy(col("prediction").desc)))
      .filter(s"tmp <= ${algParameters.recommendSize}").drop("tmp")
    modelOutput.reorderedDataFrame = result
  }

  override protected def afterInvoke(): Unit = {

  }



}

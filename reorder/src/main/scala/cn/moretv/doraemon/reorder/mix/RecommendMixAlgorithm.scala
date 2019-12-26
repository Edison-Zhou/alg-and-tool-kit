package cn.moretv.doraemon.reorder.mix

import cn.moretv.doraemon.common.alg.Algorithm
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


/**
  * Created by lituo on 2018/6/20.
  * 推荐结果合并算法
  * 输入： 1. 任意多个推荐结果，表包含三列[推荐的对象,推荐的内容,评分],也可以缺省评分列
  * 对应的数据输入key是result1, result2,...,result10,..., 优先级递减
  *       2. 推荐的对象全量，表只有[推荐的对象]一列，数据输入key是total。不输入此列则不进行过滤
  * 参数： 最多推荐数，各列命名等
  * 算法逻辑: stacking -> 按优先级合并, 根据推荐优先级从高到底，堆叠满位置
  * score -> 评分全局排序，所以的结果直接根据评分排序
  * ratio -> 按比例合并，按照指定的数量依次堆叠
  * random -> 随机排序
  */
class RecommendMixAlgorithm extends Algorithm {
  override protected val algParameters: RecommendMixParameter = new RecommendMixParameter
  override protected val modelOutput: RecommendMixModel = new RecommendMixModel
  //数据Map的Key定义
  val INPUT_DATA_KEY_PREFIX = "result"
  val INPUT_DATA_KEY_TOTAL = "total"

  //临时字段名
  private val TEMP_SCORE = "temp_col_score"
  private val TEMP_RANK = "temp_col_rank"

  //数据是否传入的推荐对象全量
  private var totalProvided = true
  //输入的推荐结果数量
  private var inputCount: Int = 0

  override protected def beforeInvoke(): Unit = {
    val invalidInput = dataInput.keys.filter(k => !k.equals(INPUT_DATA_KEY_TOTAL) && !k.startsWith(INPUT_DATA_KEY_PREFIX))
    if (invalidInput.nonEmpty) {
      throw new IllegalArgumentException("不合法的输入数据key值：" + invalidInput)
    }
    inputCount = dataInput.keys.count(k => k.startsWith(INPUT_DATA_KEY_PREFIX))
    totalProvided = dataInput.keySet.contains(INPUT_DATA_KEY_TOTAL)
    if (MixModeEnum.RATIO.equals(algParameters.mixMode)) {
      if (algParameters.ratio == null || algParameters.ratio.length != inputCount) {
        throw new IllegalArgumentException("堆叠比例参数ratio的长度必须跟输入的数据长度一致")
      }
    }

  }

  override protected def invoke(): Unit = {
    var df = computeScore(dataInput(INPUT_DATA_KEY_PREFIX + "1"), algParameters.mixMode, 1)
    if (inputCount > 1) {
      2.to(inputCount).foreach(c =>
        df = df.union(computeScore(dataInput(INPUT_DATA_KEY_PREFIX + c), algParameters.mixMode, c))
      )
    }

    val user = algParameters.recommendUserColumn
    val item = algParameters.recommendItemColumn
    val score = algParameters.scoreColumn

    df = df.groupBy(user, item).agg(max(score).as(score), max(TEMP_SCORE).as(TEMP_SCORE)) //去重
      .withColumn(TEMP_RANK, row_number().over(Window.partitionBy(user).orderBy(col(TEMP_SCORE).desc))) //编号
      .filter(TEMP_RANK + " <= " + algParameters.recommendNum) //过滤
      .drop(TEMP_RANK) //删除辅助字段

    //根据配置处理score字段
    df = if(!algParameters.outputOriginScore)
      df.withColumn(score, col(TEMP_SCORE)).drop(TEMP_SCORE)
    else df.drop(TEMP_SCORE)

    //基于total过滤最终结果
    modelOutput.mixResult =
      if (totalProvided) {
        df.as("a").join(dataInput(INPUT_DATA_KEY_TOTAL).as("b"), expr(s"a.$user = b.$user"), "leftouter")
          .where(s"b.$user is not null")
          .selectExpr("a.*")
      } else {
        df
      }
  }

  private def computeScore(df: DataFrame, mode: MixModeEnum.Value, tableIndex: Int): DataFrame = {
    //没有score字段的情况，增加score字段
    val containScoreColumn = df.schema.fields.map(f => f.name).contains(algParameters.scoreColumn)
    val newDf = if (!containScoreColumn) df.withColumn(algParameters.scoreColumn, lit(0.0)) else df
    mode match {
      case MixModeEnum.STACKING =>
        //把score归一化到0到1之间
        val normalizeScoreDf = normalizeScore(newDf)
        //再加上（总数-编号）,保证1号推荐结果的所有分数都比2号高，依次类推
        normalizeScoreDf.withColumn(TEMP_SCORE, expr(s"$TEMP_SCORE + $inputCount - $tableIndex"))

      case MixModeEnum.SCORE => newDf.withColumn(TEMP_SCORE, col(algParameters.scoreColumn))

      case MixModeEnum.RANDOM => newDf.withColumn(TEMP_SCORE, rand())

      case MixModeEnum.RATIO =>
        //把score归一化到0到1之间
        val normalizeScoreDf = normalizeScore(newDf)
        //每次在当前推荐结果中取几个
        val ratio = algParameters.ratio(tableIndex - 1)
        val user = algParameters.recommendUserColumn
        normalizeScoreDf
          .withColumn(TEMP_RANK, row_number().over(Window.partitionBy(user).orderBy(col(TEMP_SCORE).desc))) //编号
          .withColumn(TEMP_RANK, expr(s"($TEMP_RANK - 1) / $ratio")) //分组
          .withColumn(TEMP_SCORE, expr(s"$TEMP_SCORE - ($TEMP_RANK * $inputCount + $tableIndex)"))
          .drop(TEMP_RANK)

      case _ => throw new IllegalArgumentException("不支持的混合类型:" + mode)
    }
  }

  /**
    * 归一化score，增加临时列
    *
    * @param df
    * @return
    */
  private def normalizeScore(df: DataFrame): DataFrame = {
    val scoreColumn = algParameters.scoreColumn
    val row = df.agg(min(scoreColumn).as("min_score"), max(scoreColumn).as("max_score")).first()
    val maxScore: Double = row.getAs[Double]("max_score")
    val minScore: Double = row.getAs[Double]("min_score")
    if (minScore >= 0 && minScore < 1 && maxScore >= 0 && maxScore < 1) {
      df.withColumn(TEMP_SCORE, expr(scoreColumn))
    } else {
      df.withColumn(TEMP_SCORE, expr(s"($scoreColumn - $minScore)*1.0/($maxScore - $minScore + 0.000001)"))
    }
  }

  override protected def afterInvoke(): Unit = {}
}

package cn.moretv.doraemon.reorder.mix

import cn.moretv.doraemon.common.alg.AlgParameters
import cn.moretv.doraemon.common.path.HdfsPath


/**
  * Created by lituo on 2018/6/20.
  */
class RecommendMixParameter extends AlgParameters{
  /**
    * 算法名称
    */
  override val algName = "mix"

  /**
    * 每个推荐对象最多推荐的数量
    */
  var recommendNum: Int = 20

  /**
    * 推荐对象
    */
  var recommendUserColumn: String = "user"

  /**
    * 推荐内容字段名
    */
  var recommendItemColumn: String = "item"

  var scoreColumn: String = "score"

  /**
    * 是否输出原始score值字段
    * 设置为否，则根据mix类型输出按照排序从前到后依次变小的score
    */
  var outputOriginScore: Boolean = true

  /**
    * 混合模式，具体包括
    * stacking -> 按优先级合并
    * score -> 评分全局排序
    * ratio -> 按比例合并
    * random -> 随机排序
    * 具体实现方式见算法实现类
    */
  var mixMode: MixModeEnum.Value = MixModeEnum.STACKING

  /**
    * 融合数量，array长度需要跟输入的推荐结果一致
    */
  var ratio: Array[Int] = _

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
    if(recommendNum <= 0) {
      return "最多推荐的数量必须是正数"
    }
    if(recommendUserColumn == null || recommendUserColumn.isEmpty) {
      return "推荐对象字段名不能为空"
    }
    if(recommendItemColumn == null || recommendItemColumn.isEmpty) {
      return "推荐内容字段名不能为空"
    }
    if(mixMode.equals(MixModeEnum.SCORE) && (scoreColumn == null || scoreColumn.isEmpty())) {
      return "score模式分数字段名不能为空"
    }
    if(scoreColumn == null || scoreColumn.isEmpty()) {
      return "分数字段名称不能为空"
    }
    "ok"
  }
}

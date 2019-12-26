package cn.moretv.doraemon.data.writer

import cn.moretv.doraemon.common.enum.FormatTypeEnum

/**
  * Created by lituo on 2018/6/27.
  */
class DataPackParam extends Serializable {

  /**
    * 额外需要添加到结果数据中的字段, 默认为空
    */
  var extraValueMap: Map[String, Any] = _

  /**
    * 给key增加前缀，默认为空
    */
  var keyPrefix: String = _

  /**
    * 打包格式，默认是键值对
    */
  var format: FormatTypeEnum.Value = FormatTypeEnum.KV

  var zsetAlg: String = _

}

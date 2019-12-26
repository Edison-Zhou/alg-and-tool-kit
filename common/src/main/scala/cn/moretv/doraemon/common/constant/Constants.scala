package cn.moretv.doraemon.common.constant

/**
  * Created by lituo on 2018/6/13.
  */
object Constants {

  val MODEL_PATH_BASE = "/ai/model/"
  val MODEL_INFO_FILE = "model_info"
  val MODEL_PARAM_FILE = "model_param"
  val MODEL_PATH_BASE_DEBUG = "/ai/tmp/model/"

  val OUTPUT_PATH_BASE = "/ai/output/"
  val OUTPUT_PATH_BASE_DEBUG = "/ai/tmp/output/"

  val DATA_PATH_BASE = "/ai/data/"
  val DATA_PATH_BASE_DEBUG = "/ai/tmp/data/"


  //日期dateFormat规则
  val DATE_YYYYMMDD= "yyyyMMdd"
  val DATE_YYYY_MM_DD= "yyyy-MM-dd"

  //输入path路径日期时间正则标识
  val INPUT_PATH_DATE_PATTERN = "#{date}"

  //产品线
  val PRODUCT_LINE_MORETV = "moretv"
  val PRODUCT_LINE_HELIOS = "helios"

  //数组操作方式
  val ARRAY_OPERATION_TAKE = "take"
  val ARRAY_OPERATION_TAKE_AND_RANDOM = "take_and_random"
  val ARRAY_OPERATION_RANDOM = "random"


}

package cn.moretv.doraemon.common.enum


/**
  * Created by guohao on 2018/6/22.
  */
/**
  * 执行环境枚举类
  */
object EnvEnum extends Enumeration{
  type envType = Value
  /**
    * PRO:生产环境
    * PRE:预发布环境
    * TEST:测试环境
    * DEV:开发环境
    */
  val PRO, PRE, TEST, DEV = Value
}

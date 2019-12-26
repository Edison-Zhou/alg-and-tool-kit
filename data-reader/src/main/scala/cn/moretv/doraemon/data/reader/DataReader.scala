package cn.moretv.doraemon.data.reader

import cn.moretv.doraemon.common.enum.EnvEnum
import cn.moretv.doraemon.common.path._
import cn.whaley.sdk.utils.TransformUDF
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by guohao on 2018/6/22.
  *
  *
  */
/**
  * 数据读取类
  *
  * @param environment 环境参数
  */
class DataReader(environment: EnvEnum.Value) {

  def this() {
    this(EnvEnum.PRO)
  }

  /**
    * 数据读取加载入口
    *
    * @param path
    * @return
    */
  def read(path: Path): DataFrame = {
    implicit val ss:SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    TransformUDF.registerUDFSS
    path match {
      case hp: HdfsPath => {
        HdfsReader.read(hp)
      }
      case mp: MysqlPath => {
        MysqlReader.read(mp)
      }
      case hivePath: HivePath=> {
        HiveReader.read(hivePath)
      }
      case kp: KeyedPath => {
        KeyedReader.read(kp)
      }
      case _ => throw new Exception("请传入正确的Path对象")
    }
  }

}




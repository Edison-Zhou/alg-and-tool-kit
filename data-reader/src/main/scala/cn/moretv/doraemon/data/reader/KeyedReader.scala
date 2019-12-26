package cn.moretv.doraemon.data.reader

import cn.moretv.doraemon.common.path.{KeyedPath, Path}
import cn.moretv.doraemon.data.keyed.{ConfigParser, HdfsYamlConfigParser}
import org.apache.spark.sql.DataFrame

/**
  * Created by lituo on 2018/7/2.
  */
object KeyedReader {

  /**
    * 数据读取加载入口
    *
    * @param path
    * @return
    */
  def read(path: KeyedPath): DataFrame = {
    val newPath = getDataPath(path)
    val dataReader = new DataReader()
    dataReader.read(newPath)
  }

  private def getDataPath(path: KeyedPath): Path = {
    val config: ConfigParser = new HdfsYamlConfigParser
    config.parse(path.key, path.paramMap, path.placeholderValueMap)
  }
}

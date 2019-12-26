package cn.moretv.doraemon.data.keyed

import cn.moretv.doraemon.common.enum.FileFormatEnum
import cn.moretv.doraemon.common.path._

import scala.collection.JavaConverters._

/**
  * 读取配置并解析成对应path对象
  * @author lituo
  * @since 2018/7/2
  */
trait ConfigParser {

  /**
    * 读取配置并解析成对应path对象
    * @param key 配置名称
    * @param paramMap 覆盖参数
    * @return
    */
  final def parse(key: String, paramMap: Map[String, Any]): Path = {
    parse(key, paramMap, null)
  }

  /**
    * 读取配置并解析成对应path对象
    * @param key 配置名称
    * @param paramMap 覆盖参数
    * @param placeholderValueMap 占位符参数
    * @return
    */
  final def parse(key: String, paramMap: Map[String, Any], placeholderValueMap: Map[String, String]): Path = {
    var config = readConfig(key)
    if (paramMap != null) {
      //自定义的配置替换提前保存的配置
      config = config ++ paramMap
    }
    parseConfig(config, placeholderValueMap)
  }

  protected final def parseConfig(config: Map[String, Any], placeholderValueMap: Map[String, String]): Path = {
    val pathType = config.get("pathType")
    //替换占位符
    val configWithValue = config.mapValues {
      case str: String if str.contains("${") =>
        val pattern = "\\$\\{(\\w+)\\}".r
        (pattern findAllIn str).foreach(placeholder => {
          val placeholderKey = placeholder.substring(2, placeholder.length - 1)
          placeholderValueMap.get(placeholderKey) match {
            case Some(value: String) => str.replace(placeholder, value)
            case _ => throw new IllegalArgumentException("")
          }
        })
      case value =>
        value
    }
    pathType match {
      case Some("mysql") => makeMySqlPath(configWithValue)
      case Some("hdfs") => makeHdfsPath(configWithValue)
      case Some("hive") => makeHivePath(configWithValue)
      case Some(errorPathType) => throw new RuntimeException("不合法的路径类型:" + errorPathType)
      case _ => throw new RuntimeException("缺少路径类型字段")
    }
  }

  private def makeMySqlPath(config: Map[String, Any]): MysqlPath = {
    MysqlPath(config.getOrElse("host", null).asInstanceOf[String],
      config.getOrElse("port", null).asInstanceOf[Int],
      config.getOrElse("database", null).asInstanceOf[String],
      config.getOrElse("tableName", null).asInstanceOf[String],
      config.getOrElse("user", null).asInstanceOf[String],
      config.getOrElse("password", null).asInstanceOf[String],
      config.getOrElse("parField", null).asInstanceOf[String],
      config.getOrElse("selectFiled", null).asInstanceOf[Array[String]],
      config.getOrElse("filterCondition", null).asInstanceOf[String],
      config.getOrElse("advancedSql", null).asInstanceOf[String]
    )
  }

  private def makeHdfsPath(config: Map[String, Any]): HdfsPath = {
    val drMap = config.getOrElse("dr", null).asInstanceOf[java.util.LinkedHashMap[String, Any]].asScala
    val dr: DateRange = DateRange(
      drMap.getOrElse("dateFormat", null).asInstanceOf[String],
      drMap.getOrElse("startDate", null).asInstanceOf[String],
      drMap.getOrElse("endDate", null).asInstanceOf[String],
      drMap.getOrElse("numberDays", null).asInstanceOf[Int]
    )
    val fileType = config.getOrElse("fileType", null).asInstanceOf[String]
    HdfsPath(dr,
      config.getOrElse("hdfsPath", null).asInstanceOf[String],
      if (fileType != null && fileType.trim.nonEmpty) FileFormatEnum.withName(fileType) else null,
      config.getOrElse("sql", null).asInstanceOf[String]
    )
  }

  private def makeHivePath(config: Map[String, Any]): HivePath = {
    HivePath(config.getOrElse("sql", null).asInstanceOf[String])
  }

  protected def readConfig(key: String): Map[String, Any]

}

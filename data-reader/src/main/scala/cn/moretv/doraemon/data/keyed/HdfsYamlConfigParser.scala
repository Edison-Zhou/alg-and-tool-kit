package cn.moretv.doraemon.data.keyed

import cn.moretv.doraemon.common.util.HdfsUtil
import org.yaml.snakeyaml.Yaml

import scala.collection.JavaConverters._


/**
  * @author lituo
  * @since 2018/7/2
  */
class HdfsYamlConfigParser extends ConfigParser {

  private val HDFS_CONFIG_PATH = "/tmp/config/"

  override def readConfig(key: String): Map[String, Any] = {
    val conf = HdfsUtil.getHDFSFileContent(HDFS_CONFIG_PATH + key + ".yml")
    val yaml = new Yaml
    val config = yaml.load[java.util.LinkedHashMap[String, Any]](conf)
    config.asScala.toMap
  }

}

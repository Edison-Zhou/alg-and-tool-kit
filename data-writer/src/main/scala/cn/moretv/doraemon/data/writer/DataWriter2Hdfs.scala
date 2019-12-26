package cn.moretv.doraemon.data.writer

import cn.moretv.doraemon.common.constant.Constants
import cn.moretv.doraemon.common.path.{HdfsPath, Path}
import cn.moretv.doraemon.common.util.HdfsUtil
import org.apache.spark.sql.DataFrame

import scala.reflect.io.File

/**
  * Created by lituo on 2018/7/23.
  */
class DataWriter2Hdfs extends DataWriter {
  override def write(df: DataFrame, path: Path): Unit = {
    path match {
      case hdfsPath: HdfsPath =>
        val pathPrefix = hdfsPath.getHdfsPath()
        if(!pathPrefix.startsWith(Constants.OUTPUT_PATH_BASE)
          && !pathPrefix.startsWith(Constants.OUTPUT_PATH_BASE_DEBUG)
          && !pathPrefix.startsWith(Constants.DATA_PATH_BASE)
          && !pathPrefix.startsWith(Constants.DATA_PATH_BASE_DEBUG)) {
          throw new RuntimeException("必须输出到指定的目录中")
        }
        if (!HdfsUtil.pathIsExist(pathPrefix)) {
          HdfsUtil.createDir(pathPrefix)
        }
        HdfsUtil.deleteHDFSFileOrPath(pathPrefix + File.separator + "Transfer")
        df.write.parquet(pathPrefix + File.separator + "Transfer")
        HdfsUtil.deleteHDFSFileOrPath(pathPrefix + File.separator + "BackUp")
        if (HdfsUtil.pathIsExist(pathPrefix + File.separator + "Latest")) {
          HdfsUtil.rename(pathPrefix + File.separator + "Latest", pathPrefix + File.separator + "BackUp")
        }
        HdfsUtil.rename(pathPrefix + File.separator + "Transfer", pathPrefix + File.separator + "Latest")
      case _ => throw new Exception("请传入正确的Path类,hdfsPath")
    }
  }
}

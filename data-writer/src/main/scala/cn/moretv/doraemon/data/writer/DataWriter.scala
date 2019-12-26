package cn.moretv.doraemon.data.writer

import cn.moretv.doraemon.common.path.Path
import org.apache.spark.sql.DataFrame

/**
  * Created by baozhiwang on 2018/6/11.
  */
trait DataWriter {
  def write(df:DataFrame,path:Path)
}

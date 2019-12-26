package cn.moretv.doraemon.test

import cn.moretv.doraemon.common.path.HdfsPath
import cn.moretv.doraemon.common.util.HdfsUtil
import cn.moretv.doraemon.data.writer.DataWriter2Hdfs


/**
  * Created by lituo on 2018/8/15.
  */
object HdfsTest extends BaseClass {
  override def execute(): Unit = {

    val sqlC = sqlContext
    import sqlC.implicits._

    val df1 =  Seq(
      ("a", "b", 0.1),
      ("a", "c", 0.3),
      ("a", "d", 3.0),
      ("a1", "b", 0.1),
      ("a1", "c", 0.3),
      ("a1", "d", 3.0)
    ).toDF("u", "s", "score")

    new DataWriter2Hdfs().write(df1, new HdfsPath("/tmp/model/medusa/test"))
  }
}

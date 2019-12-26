package cn.moretv.doraemon.data.reader

import cn.moretv.doraemon.common.path.HivePath
import cn.moretv.doraemon.data.reader.HiveReader
import org.apache.spark.sql.SparkSession
import org.junit.{Before, Test}

/**
  * Created by guohao on 2018/7/6.
  */
class HiveReaderTest {

  @Before
  def init(): Unit ={
    val ss:SparkSession = SparkSession.builder().master("local[2]").getOrCreate()
  }

  @Test
  def test(): Unit ={
//    val sql = "select * from `ods_view`.`db_snapshot_mysql_medusa_mtv_program` where key_day='20181008' and contenttype='movie' and status = 1 and type = 1 "
    val sql = "select * from `ods_view`.`db_snapshot_mysql_medusa_mtv_program` where key_day='20181008' and contenttype='movie' and status = 1 and type = 1 "
    val hivePath = new HivePath(sql)
    val df = HiveReader.read(hivePath)
    df.show()
  }
}

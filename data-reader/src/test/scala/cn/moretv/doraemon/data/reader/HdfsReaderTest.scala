package cn.moretv.doraemon.data.reader

import cn.moretv.doraemon.common.constant.Constants
import cn.moretv.doraemon.common.enum.FileFormatEnum
import cn.moretv.doraemon.common.path.{DateRange, HdfsPath}
import cn.moretv.doraemon.data.reader.HdfsReader
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.junit.{Before, Test}
/**
  * Created by guohao on 2018/6/21.
  */
class HdfsReaderTest {
  var hdfsPath : HdfsPath = null

  @Before
  def init(): Unit ={
    val sparkConf:SparkConf = new SparkConf()
    sparkConf.setMaster("local[2]")
    val ss:SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
  }

  def init1(): Unit ={
    val path = "/data_warehouse/ods_origin.db/log_origin/main.json"
    val path2 = "/data_warehouse/ods_view.db/db_snapshot_medusa_sailfish_sport_match/key_day=20180619"
     hdfsPath = new HdfsPath(path2,FileFormatEnum.PARQUET)
  }
  @Test
  def init2(): Unit ={
    val dr = new DateRange(Constants.DATE_YYYYMMDD,1)
//    val dr = new DateRange(Constants.DATE_YYYYMMDD,"20181010","20181012")
    val path = "/ai/etl/ai_base_behavior_display/product_line=moretv/biz=portalRecommend_ex/key_time=#{date}"
//    hdfsPath = new HdfsPath(dr,path)
    hdfsPath = new HdfsPath(dr,path,"select id from tmp")
    val df = HdfsReader.read(hdfsPath)
    df.show(10)
  }

  def int3(): Unit ={
    val path = "/tmp/test.csv"
    val path2 = "/data_warehouse/ods_view.db/db_snapshot_medusa_sailfish_sport_match/key_day=20180619"
    hdfsPath = new HdfsPath(path,FileFormatEnum.CSV)
  }

  def int4(): Unit ={
    val numDaysOfData = new DateRange("yyyyMMdd",1)
    val path = "/ai/etl/ai_base_behavior_raw/product_line=moretv/partition_tag=#{date}/score_source=play/useridoraccountid=1"

    hdfsPath =  new HdfsPath(numDaysOfData,path)
  }
  @Test
  def test(): Unit ={
    int4()
    val df = HdfsReader.read(hdfsPath)
    df.schema.fields.foreach(f=>{
      println(s"name:${f.name} type:${f.dataType}")
    })
    df.show()
  }




}

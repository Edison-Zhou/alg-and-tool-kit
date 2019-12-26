package cn.moretv.doraemon.data.reader

import cn.moretv.doraemon.common.constant.Constants
import cn.moretv.doraemon.common.enum.FileFormatEnum
import cn.moretv.doraemon.common.path.{DateRange, HdfsPath}
import cn.moretv.doraemon.data.reader.DataReader
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.junit.{Before, Test}

/**
  * Created by guohao on 2018/6/22.
  */
class DataReaderForHdfsTest {

  @Before
  def init(): Unit ={
    val sparkConf:SparkConf = new SparkConf()
    sparkConf.setMaster("local[2]")
    val ss:SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
  }

  @Test
  def test(): Unit ={
    val path = "/data_warehouse/ods_view.db/db_snapshot_medusa_sailfish_sport_match/key_day=20180619"
    val hdfsPath =  new HdfsPath(path)
    val dataReader = new DataReader()
    val df = dataReader.read(hdfsPath)
    df.show(5)
  }



  @Test
  def test2(): Unit ={
    val path = "/data_warehouse/ods_origin.db/log_origin/main.json"
    val hdfsPath =  new HdfsPath(path,FileFormatEnum.JSON)
    val dataReader = new DataReader()
    val df = dataReader.read(hdfsPath)
    df.show(5)
  }

  @Test
  def test22(): Unit ={
    val path = "/data_warehouse/ods_origin.db/log_origin/main.json"
    val sql = "select msgBody from tmp "
    val hdfsPath =  new HdfsPath(path,FileFormatEnum.JSON,sql)
    val dataReader = new DataReader()
    val df = dataReader.read(hdfsPath)
    df.show(5)
  }


  @Test
  def test3(): Unit ={
    val path = "/data_warehouse/ods_view.db/db_snapshot_medusa_sailfish_sport_match/key_day=#{date}/key_hour=00"
    val dr = new DateRange(Constants.DATE_YYYYMMDD,-3)
    val hdfsPath =  new HdfsPath(dr,path)
    val dataReader = new DataReader()
    val df = dataReader.read(hdfsPath)
    df.show(5)
  }

  @Test
  def test4(): Unit ={
    val path = "/data_warehouse/ods_view.db/db_snapshot_medusa_sailfish_sport_match/key_day=#{date}/key_hour=00"
    val dr = new DateRange(Constants.DATE_YYYYMMDD,"20180620","20180622")
    val hdfsPath =  new HdfsPath(dr,path)
    val dataReader = new DataReader()
    val df = dataReader.read(hdfsPath)
    df.show(5)
  }

  @Test
  def test5(): Unit ={
    val path = "/data_warehouse/ods_view.db/db_snapshot_medusa_sailfish_sport_match/key_day=#{date}/key_hour=00"
    val dr = new DateRange(Constants.DATE_YYYYMMDD,"20180620","20180622")
    val sql = "select id from tmp"
    val hdfsPath =  new HdfsPath(dr,path,sql)
    val dataReader = new DataReader()
    val df = dataReader.read(hdfsPath)
    df.show(5)
  }


}

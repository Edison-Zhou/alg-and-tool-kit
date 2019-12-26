package cn.moretv.doraemon.data.reader

import cn.moretv.doraemon.common.path.KeyedPath
import cn.moretv.doraemon.data.keyed.HdfsYamlConfigParser
import cn.moretv.doraemon.data.reader.DataReader
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.junit.{Assert, Before, Test}

/**
  * Created by lituo on 2018/7/2.
  */
class KeyedReaderTest {

  @Before
  def init(): Unit ={
    val sparkConf:SparkConf = new SparkConf()
    sparkConf.setMaster("local[2]")
    val ss:SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
  }

  @Test
  def test(): Unit ={
    val configParser = new HdfsYamlConfigParser
    val config = configParser.parse("test",null)
    Assert.assertTrue(config != null)
  }



  @Test
  def readTest(): Unit ={
    val dataReader = new DataReader
    val result = dataReader.read(KeyedPath("test", null, null))
    Assert.assertTrue(result != null)
  }
}

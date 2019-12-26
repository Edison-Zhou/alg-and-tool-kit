package cn.moretv.doraemon.data.reader

import cn.moretv.doraemon.common.path.MysqlPath
import cn.moretv.doraemon.data.reader.{DataReader, MysqlReader}
import org.apache.spark.sql.SparkSession
import org.junit.{Before, Test}

/**
  * Created by guohao on 2018/6/28.
  */
class DataReaderForMysqlTest {

  implicit val ss = SparkSession.builder().master("local[2]").getOrCreate()
  var host:String = null
  var port:Integer = null
  var database:String = null
  var tableName:String = null
  var user:String = null
  var password:String = null
  var parField:String = null
  var selectFiled:Array[String] = null
  var filterContition:String = null
  @Before
  def init(): Unit ={
    host = "bigdata-appsvr-130-7"
    port = 3306
    database = "dbSnapShot"
    tableName = "db_snapshot_table_info"
    user = "bigdata"
    password = "bigdata@whaley666"
    parField = "id"
    selectFiled = Array("id","db_name")
    filterContition = " status = 1 "
  }

  @Test
  def test(): Unit ={
    val mysqlPath = new MysqlPath(host,port,database,tableName,user,password,parField,selectFiled,filterContition)
    val dataReader = new DataReader()
    val df = dataReader.read(mysqlPath)
    df.show(100)
  }

  @Test
  def test2(): Unit ={
    val mysqlPath = new MysqlPath(host,port,database,tableName,user,password)
    val dataReader = new DataReader()
    val df = dataReader.read(mysqlPath)
    df.show(100)
  }

  @Test
  def test3(): Unit ={
    val mysqlPath = new MysqlPath(host,port,database,tableName,user,password,selectFiled)
    val dataReader = new DataReader()
    val df = dataReader.read(mysqlPath)
    df.show(100)
  }

  @Test
  def test4(): Unit ={
    val mysqlPath = new MysqlPath(host,port,database,tableName,user,password,selectFiled,filterContition)
    val dataReader = new DataReader()
    val df = dataReader.read(mysqlPath)
    df.show(100)
  }

  @Test
  def test5(): Unit ={
    implicit val ss = SparkSession.builder().master("local[2]").getOrCreate()
    val mysqlPath = new MysqlPath("bigdata-appsvr-130-4", 3306,
      "tvservice", "mtv_program", "bislave", "slave4bi@whaley", "id",
      Array("sid"),
      s"content_type = 'comic' and sid is not null " +
        "and title is not null and status = 1 and type = 1","select transformSid(sid) as sid from tmp")
    val dataReader = new DataReader()
    val df = dataReader.read(mysqlPath)
    df.show(100)

  }
}

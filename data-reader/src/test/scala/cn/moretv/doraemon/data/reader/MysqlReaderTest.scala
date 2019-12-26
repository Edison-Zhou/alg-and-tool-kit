package cn.moretv.doraemon.data.reader

import cn.moretv.doraemon.common.path.MysqlPath
import cn.moretv.doraemon.data.reader.MysqlReader
import org.apache.spark.sql.SparkSession
import org.junit.{Before, Test}



/**
  * Created by guohao on 2018/6/27.
  */
class MysqlReaderTest {
  val ss = SparkSession.builder().master("local[2]").getOrCreate()
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
    val df = MysqlReader.read(mysqlPath)
    df.show(100)
  }

  @Test
  def test2(): Unit ={
    val mysqlPath = new MysqlPath(host,port,database,tableName,user,password)
    val df = MysqlReader.read(mysqlPath)
    df.show(100)
  }

  @Test
  def test3(): Unit ={
    val mysqlPath = new MysqlPath(host,port,database,tableName,user,password,selectFiled)
    val df = MysqlReader.read(mysqlPath)
    df.show(100)
  }

  @Test
  def test4(): Unit ={
    val mysqlPath = new MysqlPath(host,port,database,tableName,user,password,selectFiled,filterContition)
    val df = MysqlReader.read(mysqlPath)
    df.show(100)
  }




}

package cn.moretv.doraemon.data.util

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.{List,Properties}

import org.apache.commons.dbutils.{DbUtils, QueryRunner}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD

import java.lang.{Double => JDouble, Float => JFloat, Long => JLong, Short => JShort}

import scala.reflect.ClassTag
import MySqlOps._
import org.apache.commons.dbutils.handlers.{ArrayHandler, ArrayListHandler}

/**
 * Created by xutong on 2016/3/11.
 * mysql的工具类
 */
class MySqlOps(prop:Properties) extends Serializable {

  /**
   * 定义变量
   */
  val queryRunner: QueryRunner = new QueryRunner

  val driver:String = prop.getProperty("driver")
  val url:String = prop.getProperty("url")
  val user:String = prop.getProperty("user")
  val password:String = prop.getProperty("password")
  Class.forName(driver)
  private lazy val conn: Connection = DriverManager.getConnection(url,user,password)




  /**
   * 通过指定的SQL语句和参数查询数据
    *
    * @param sql 预编译的sql语句
   * @param params 查询参数
   * @return 查询结果
   */
   def selectOne(sql: String, params: Any*):Array[AnyRef] = {
    queryRunner.query(conn, sql, new ArrayHandler(), asScala(params): _*)
  }



  /**
   * 通过指定的SQL语句和参数查询数据
    *
    * @param sql 预编译的sql语句
   * @param params 查询参数
   * @return 查询结果
   */
  def selectArrayList(sql: String, params: Any*): List[Array[AnyRef]] = {
    queryRunner.query(conn, sql, new ArrayListHandler(), asScala(params): _*)
  }



  /**
   * 查询ID号
    *
    * @param column　列名
   * @param tableName 表名
   * @return  最大最小ID
   */
  def queryMaxMinID(tableName: String,column: String = "id" ):(Long,Long)={
    val sql = s"select MIN($column),MAX($column) from $tableName"
    val statement = conn.createStatement()
    val result =  statement.executeQuery(sql)
    result.next()
    val minID = result.getLong(1)
    val maxID = result.getLong(2)
    result.close()
    (minID,maxID)
  }

  /**
   * 查询ID号
    *
    * @param sql　查询语句
   * @return  最大最小ID
   */
  def queryMaxMinIDBySql(sql: String ):(Long,Long)={
    val statement = conn.createStatement()
    val result =  statement.executeQuery(sql)
    result.next()
    val minID = result.getLong(1)
    val maxID = result.getLong(2)
    result.close()
    (minID,maxID)
  }
  /**
   * 释放资源，如关闭数据库连接
   */
  def destory() {
    DbUtils.closeQuietly(conn)
  }

}

object MySqlOps{

  def apply(prop:Properties): MySqlOps = new MySqlOps(prop)

  def getJdbcRDD[T: ClassTag](sc: SparkContext,sql: String,table: String,func:(ResultSet=>T),driver:String,url:String,user:String,password:String,maxMinID:(Long,Long),numPartition: Int):JdbcRDD[T]={

    new JdbcRDD[T](sc,()=>{
      Class.forName(driver)
      DriverManager.getConnection(url,user,password)
    },
      sql,
      maxMinID._1,
      maxMinID._2,
      numPartition,
      func)
  }

  def getJdbcRDD[T: ClassTag](sc: SparkContext,prop:Properties,sqlMinMaxId: String,sqlData:String,
                              numPartition: Int,func:(ResultSet=>T)):JdbcRDD[T]={

    val (driver,url,user,password) = getMysqlConf(prop)
    val db = MySqlOps(prop)
    val ids = db.selectOne(sqlMinMaxId)
    val minId = ids(0).toString.toLong
    val maxId = ids(1).toString.toLong

    new JdbcRDD[T](sc,()=>{
      Class.forName(driver)
      DriverManager.getConnection(url,user,password)
    },
      sqlData,
      minId,
      maxId,
      numPartition,
      func)
  }

  def getJdbcRDDBySql[T: ClassTag](sc: SparkContext,prop:Properties,maxMinID:(Long,Long),sql: String,numPartition: Int,func:(ResultSet=>T)):JdbcRDD[T]={
    val (driver,url,user,password) = getMysqlConf(prop)
    new JdbcRDD[T](sc,()=>{
      Class.forName(driver)
      DriverManager.getConnection(url,user,password)
    },
      sql,
      maxMinID._1,
      maxMinID._2,
      numPartition,
      func)
  }

  def getJdbcRDDBySql[T: ClassTag](sc: SparkContext,sql: String,func:(ResultSet=>T),driver:String,url:String,user:String,password:String,maxMinID:(Long,Long),numPartition: Int):JdbcRDD[T]={
    new JdbcRDD[T](sc,()=>{
      Class.forName(driver)
      DriverManager.getConnection(url,user,password)
    },
      sql,
      maxMinID._1,
      maxMinID._2,
      numPartition,
      func)
  }

  def asScala(params: Seq[Any]) = {
    params.map{
      case null => null
      case e:Long => new JLong(e)
      case e:Double => new JDouble(e)
      case e:String => e
      case e:Short => new JShort(e)
      case e:Int => new Integer(e)
      case e:Float => new JFloat(e)
      case e:Any => e.asInstanceOf[Object]
    }
  }


  def getMysqlConf(prop:Properties) = {
    val driver = prop.getProperty("driver")
    val url = prop.getProperty("url")
    val user = prop.getProperty("user")
    val password = prop.getProperty("password")
    (driver,url,user,password)
  }
}

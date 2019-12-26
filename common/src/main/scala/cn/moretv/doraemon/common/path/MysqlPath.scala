package cn.moretv.doraemon.common.path


/**
  * Created by guohao on 2018/6/13.
  */
case class MysqlPath(host:String, port:Integer, database:String, tableName:String, user:String, password:String,
                     parField:String, selectFiled:Array[String], filterCondition:String,advancedSql:String) extends Path {

  def this(host:String, port:Integer, database:String, tableName:String, user:String, password:String,parField:String, selectFiled:Array[String], filterCondition:String){
    this(host,port,database,tableName,user,password,parField,selectFiled,filterCondition,"")
  }
  /**
    * 默认使用id作为分区字段
    * 快照全量字段和记录
    * @param host
    * @param port
    * @param database
    * @param tableName
    * @param user
    * @param password
    */
  def this(host:String,port:Integer,database:String,tableName:String,user:String,password:String){
      this(host,port,database,tableName,user,password,"id",Array("*"),"")
  }

  /**
    * 过滤可选字段
    * @param host
    * @param port
    * @param database
    * @param tableName
    * @param user
    * @param password
    * @param selectFiled
    */
  def this(host:String,port:Integer,database:String,tableName:String,user:String,password:String,selectFiled:Array[String]){
    this(host,port,database,tableName,user,password,"id",selectFiled,"")
  }


  /**
    * 过滤记录
    * @param host
    * @param port
    * @param database
    * @param tableName
    * @param user
    * @param password
    * @param filterCondition
    */
  def this(host:String, port:Integer, database:String, tableName:String, user:String, password:String, filterCondition:String){
    this(host,port,database,tableName,user,password,"id",Array("*"),filterCondition)
  }

  /**
    * 过滤字段和记录
    * @param host
    * @param port
    * @param database
    * @param tableName
    * @param user
    * @param password
    * @param selectFiled
    * @param filterCondition
    */
  def this(host:String, port:Integer, database:String, tableName:String, user:String, password:String, selectFiled:Array[String], filterCondition:String){
    this(host,port,database,tableName,user,password,"id",selectFiled,filterCondition)
  }


}

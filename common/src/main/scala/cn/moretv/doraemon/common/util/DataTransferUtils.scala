package cn.moretv.doraemon.common.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

/**
  * Created by baozhiwang on 2018/6/21.
  * 临时测试使用，后期删除，
  * @deprecated
  */
class DataTransferUtils(ss:SparkSession) {
  /**
    * 带评分推荐结果更新到HDFS
    * @param recommend RDD[(uid, Array[sid])]
    * @param path HDFS路径
    */
  def recommendWithScoreUpdate2HDFS(recommend: RDD[(Long, Array[(Int, Double)])],
                                    path: String): Unit ={
    import ss.implicits._
    val data = recommend.flatMap(x => x._2.map(y => (x._1, y._1, y._2))).toDF("uid", "sid", "score")

    dataFrameUpdate2HDFS(data, path)
  }

  /**
    * ALS训练得到的userFactor更新至HDFS
    *
    * @param userFactors 用户特征因子
    * @param path HDFS路径
    */
  def userALSFactorsUpdate2HDFS(userFactors: RDD[(Long, Array[Float])],
                                path: String): Unit ={
    import ss.implicits._
    val data = userFactors.map(e => (e._1, e._2.map(r => r.toDouble))).toDF("uid", "features")

    dataFrameUpdate2HDFS(data, path)
  }

  /**
    * ALS训练得到的itemFactors更新至HDFS
    *
    * @param itemFactors 视频特征因子
    * @param path HDFS路径
    */
  def itemALSFactorsUpdate2HDFS(itemFactors: RDD[(Int, Array[Float])],
                                path: String): Unit ={
    import ss.implicits._
    val data = itemFactors.map(e => (e._1, e._2.map(r => r.toDouble))).toDF("sid", "features")

    dataFrameUpdate2HDFS(data, path)
  }

  /**
    * 用于将DataFrame数据更新至HDFS路径
    *
    * @param data DataFrame数据
    * @param path HDFS路径
    */
  def dataFrameUpdate2HDFS(data: DataFrame, path:String): Unit ={
    val latestPath: String = path + "/Latest"
    val backUpPath: String = path + "/BackUp"
    val transferPath: String = path + "/Transfer"

    HdfsUtil.deleteHDFSFileOrPath(transferPath)
    data.write.parquet(transferPath)

    if(HdfsUtil.pathIsExist(latestPath)){
      HdfsUtil.deleteHDFSFileOrPath(backUpPath)
      moveDir(latestPath,backUpPath)

      HdfsUtil.deleteHDFSFileOrPath(latestPath)
      moveDir(transferPath,latestPath)
    }else{
      moveDir(transferPath,latestPath)
    }

    HdfsUtil.deleteHDFSFileOrPath(transferPath)
  }


  /**
    * 移动HDFS上的路径
    *
    * @param sourceDir 源路径
    * @param targetDir 目标路径
    */
  def moveDir(sourceDir: String, targetDir: String): Unit = {

    lazy val conf = new Configuration()
    lazy val fs = FileSystem.get(conf)
    lazy val sourcePath = new Path(sourceDir)
    lazy val targetPath = new Path(targetDir)

    fs.rename(sourcePath, targetPath)
    println(s"move $sourceDir to $targetDir")
  }


}

package cn.moretv.doraemon.common.util

import java.io.{BufferedWriter, File, IOException, OutputStreamWriter}
import java.nio.charset.StandardCharsets

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

/**
  * Created by Tony on 16/12/22.
  */
object HdfsUtil {

  /**
    * 删除hdfs文件或者路径
    *
    * @param file 全路径
    * @return 成功返回true，文件已经不存在返回true
    */
  def deleteHDFSFileOrPath(file: String): Boolean = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)

    val safePath = file.replaceAllLiterally(" ", "")
    val path = new Path(safePath)
    if (fs.exists(path)) {
      fs.delete(path, true)
    } else {
      true
    }
  }

  def getHDFSFileContent(file: String): String = {
    val stream = getHDFSFileStream(file)
    try {
      IOUtils.toString(stream, StandardCharsets.UTF_8)
    } catch {
      case _: IOException => ""
    }
  }

  def getHDFSFileStream(file: String): FSDataInputStream = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val path = new Path(file)
    fs.open(path)
  }

  def writeContentToHdfs(file: String, content: String): Boolean = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)

    val fout = fs.create(new Path(file))

    var out: BufferedWriter = null
    try {
      out = new BufferedWriter(new OutputStreamWriter(fout, "UTF-8"))
      out.write(content)
      out.flush()
      true
    } catch {
      case _: Exception => false
    } finally {
      if (out != null) {
        out.close()
      }
    }
  }

  def fileIsExist(path: String, fileName: String): Boolean = {
    var flag = false
    val files = getFileFromHDFS(path)
    files.foreach(file => {
      if (file.getPath.getName == fileName) {
        flag = true
      }
    })
    flag
  }

  def getFileFromHDFS(path: String): Array[FileStatus] = {
    val dst = path
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val input_dir = new Path(dst)
    val hdfs_files = fs.listStatus(input_dir)
    hdfs_files
  }

  def pathIsExist(file: String): Boolean = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val path = new Path(file)
    fs.exists(path)
  }

  def IsDirExist(path: String): Boolean = {
    var flag = false
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    flag = fs.exists(new Path(path))
    flag
  }

  def copyFilesInDir(srcDir: String, distDir: String): Boolean = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val isSuccess = FileUtil.copy(fs, new Path(srcDir), fs, new Path(distDir), false, false, conf)
    isSuccess
  }

  def rename(src: String, dist: String): Boolean = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val srcPath = new Path(src)
    val distPath = new Path(dist)
    fs.rename(srcPath, distPath)
  }

  //check if the directory contains _SUCCESS file
  def IsInputGenerateSuccess(path: String): Boolean = {
    var flag = false
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    flag = fs.exists(new Path(path + File.separator + "_SUCCESS"))
    flag
  }


  /**
    * Make the given file and all non-existent parents into
    * directories. Has the semantics of Unix 'mkdir -p'.
    **/
  def createDir(dirName: String): Boolean = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val path = new Path(dirName)
    val flag = fs.mkdirs(path)
    flag
  }

  /**
    * 用于将DataFrame数据更新至HDFS路径
    *
    * @param data DataFrame数据
    * @param path HDFS路径
    */
  def dataFrameUpdate2HDFS(data: DataFrame, path: String): Boolean = {
    if (!pathIsExist(path)) {
      createDir(path)
    }
    val latestPath: String = path + "/Latest"
    val backUpPath: String = path + "/BackUp"
    val transferPath: String = path + "/Transfer"

    HdfsUtil.deleteHDFSFileOrPath(transferPath)
    data.write.parquet(transferPath)

    if (HdfsUtil.pathIsExist(latestPath)) {
      HdfsUtil.deleteHDFSFileOrPath(backUpPath)
      moveDir(latestPath, backUpPath)

      HdfsUtil.deleteHDFSFileOrPath(latestPath)
      moveDir(transferPath, latestPath)
    } else {
      moveDir(transferPath, latestPath)
    }

    HdfsUtil.deleteHDFSFileOrPath(transferPath)

    true
  }

  /**
    * 从HDFS上load dataFrame
    * @param path HDFS path
    * @return DataFrame
    */
  def loadDataFrameFromHDFS(path: String): DataFrame = {
    val ss: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    val latestPath: String = path + "/Latest"
    val backUpPath: String = path + "/BackUp"
    if(HdfsUtil.IsDirExist(latestPath)) {
      ss.read.parquet(latestPath)
    }else {
      require(HdfsUtil.IsDirExist(backUpPath), "The backup data doesn't exist")
      ss.read.parquet(backUpPath)
    }
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

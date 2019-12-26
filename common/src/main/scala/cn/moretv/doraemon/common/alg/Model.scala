package cn.moretv.doraemon.common.alg

import cn.moretv.doraemon.common.constant.Constants
import cn.moretv.doraemon.common.enum.{EnvEnum, ProductLineEnum}
import cn.moretv.doraemon.common.path.HdfsPath
import cn.moretv.doraemon.common.util.HdfsUtil
import org.apache.spark.SparkContext
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.mllib.util.Saveable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.SizeEstimator
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._
import org.slf4j.{Logger, LoggerFactory}

import scala.reflect.runtime.universe._

/**
  * Created by lituo on 2018/6/12.
  */
trait Model extends Serializable {

  /**
    * 模型名称
    */
  val modelName: String

  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * 保存模型到hdfs
    *
    * @param instanceName 模型实例名称，相同模型、相同实例名称在多次保存是会覆盖
    * @return
    */
  final def save(instanceName: String = "default")(implicit productLine: ProductLineEnum.Value, env: EnvEnum.Value): Boolean = {
    if (instanceName == null || instanceName.isEmpty) {
      throw new IllegalArgumentException("模型instanceName不能为空")
    }
    if (instanceName.equalsIgnoreCase("backup") || instanceName.equalsIgnoreCase("tmp")) {
      throw new IllegalArgumentException("模型instanceName不能为backup后者tmp")
    }
    val hdfsPath = getModelPathBase(env) + productLine.toString.toLowerCase + "/" + modelName + "/"
    val instancePath = hdfsPath + "/" + instanceName.toLowerCase
    val backupPathParent = hdfsPath + "backup/"
    val backupPath = hdfsPath + "backup/" + instanceName.toLowerCase
    val tmpPath = hdfsPath + "tmp/" + instanceName.toLowerCase
    HdfsUtil.deleteHDFSFileOrPath(tmpPath)
    HdfsUtil.createDir(tmpPath)

    logger.info(s"=====Save Model Start [$modelName]====")
    //保存模型类型标识文件
    val className = this.getClass.getName
    val modelInfoFilePath = tmpPath + "/" + Constants.MODEL_INFO_FILE
    val modelInfoSuccess = HdfsUtil.writeContentToHdfs(modelInfoFilePath, className)
    if (!modelInfoSuccess) {
      throw new RuntimeException("保存model基础信息失败")
    }
    //保存模型具体信息
    val result = modelContentSaveInner(tmpPath)
    //目录移动
    if (HdfsUtil.IsDirExist(backupPathParent)) {
      HdfsUtil.deleteHDFSFileOrPath(backupPath)
    } else {
      HdfsUtil.createDir(backupPathParent)
    }
    if (HdfsUtil.IsDirExist(instancePath)) {
      HdfsUtil.rename(instancePath, backupPath)
    }
    HdfsUtil.rename(tmpPath, instancePath)
    logger.info(s"=====Save Model End [$modelName], Result: $result====")
    result
  }

  /**
    * 输出模型的某个dataframe到hdfs，路径格式：/统一输出目录/产品线/业务名
    * 模型中必须只有唯一的一个dataframe
    *
    * @param bizName 业务名
    * @return
    */
  final def output(bizName: String)(implicit productLine: ProductLineEnum.Value, env: EnvEnum.Value): Boolean = {
    val fields = this.getClass.getDeclaredFields
      .filter(f => f.getType == classOf[DataFrame])

    if (fields.length != 1) {
      throw new RuntimeException(s"模型${modelName}需要只有一个dataframe才能用默认output接口")
    }

    output(bizName, fields.head.getName)
  }

  /**
    * 输出模型的某个dataframe到hdfs，路径格式：/统一输出目录/产品线/业务名
    *
    * @param bizName 业务名
    * @param dataKey 数据key, 跟model中定义的某个dataframe属性名一致
    * @return
    */
  final def output(bizName: String, dataKey: String)(implicit productLine: ProductLineEnum.Value, env: EnvEnum.Value): Boolean = {
    //todo 验证bizName合法性
    val data = this.getData(dataKey)
    val path = getOutputPathBase(env) + productLine.toString.toLowerCase + "/" + bizName
    logger.info("输出路径为：" + path)
    if (data == null) {
      throw new RuntimeException("指定的数据dataKey不存在:" + dataKey)
    } else {
      HdfsUtil.dataFrameUpdate2HDFS(data, path)
    }
  }

  /**
    * 读取规范路径下的模型
    *
    * @param instanceName 实例名称
    * @return
    */
  final def load(instanceName: String = "default")(implicit productLine: ProductLineEnum.Value, env: EnvEnum.Value): Boolean = {
    val modelPath = new HdfsPath(getModelPathBase(env) + productLine.toString.toLowerCase + "/" +
      modelName + "/" + instanceName.toLowerCase + "/")
    load(modelPath)
  }

  /**
    * 读取任意路径下的模型
    *
    * @param modelPath hdfs路径
    * @return
    */
  final def load(modelPath: HdfsPath): Boolean = {
    logger.info(s"=====Load Model Start [$modelName]====")
    val className = this.getClass.getName
    val path = modelPath.getHdfsPath() + Constants.MODEL_INFO_FILE
    val classInfo = HdfsUtil.getHDFSFileContent(path)
    if (!className.equals(classInfo)) {
      throw new RuntimeException("读取的Model类型不匹配")
    }
    val result = modelContentLoadInner(modelPath.getHdfsPath())
    logger.info(s"=====Load Model End [$modelName], Result: $result====")
    result
  }

  protected def modelContentSaveInner(modelPath: String): Boolean = {

    val fields = this.getClass.getDeclaredFields
      .filter(f => !f.getName.equals("modelName"))
      .filter(f => f.getType != classOf[Logger])

    val fieldMap = (Map[String, Any]() /: fields) { (a, f) =>
      f.setAccessible(true)
      a + (f.getName -> f.get(this))
    }

    val dfMap = fieldMap.filter(_._2.isInstanceOf[DataFrame])    //dataframe
    val saveableModelMap = fieldMap.filter(_._2.isInstanceOf[Saveable])   //mllib model
    val writeableModelMap = fieldMap.filter(_._2.isInstanceOf[MLWritable])  // ml model
    val valueMap = fieldMap.filter(f => !f._2.isInstanceOf[DataFrame]
      && !f._2.isInstanceOf[Saveable] &&  !f._2.isInstanceOf[MLWritable])   //其他model中的字段

    logger.info(s"保存DataFrame：${dfMap.size}个")
    logger.info(s"保存MLlib Model：${saveableModelMap.size}个")
    logger.info(s"保存ML Model：${dfMap.size}个")


    implicit val formats = Serialization.formats(NoTypeHints)

    //写出基本数据类型参数
    if (valueMap.nonEmpty) {
      val value = writePretty(valueMap)
      val valuePath = modelPath + "/" + Constants.MODEL_PARAM_FILE
      val success = HdfsUtil.writeContentToHdfs(valuePath, value)
      if (!success) {
        throw new RuntimeException("写入模型参数失败，" + modelName)
      }
    }

    //写出dataframe
    dfMap.foreach(m => {
      val path = modelPath + "/" + m._1
      val df = m._2.asInstanceOf[DataFrame]
      val repartitionNum = Math.ceil(SizeEstimator.estimate(df) / 10000000.0).toInt
      df.repartition(repartitionNum).write.parquet(path)
    })

    val ss: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()

    //写出MLlib Model
    saveableModelMap.foreach(m => {
      val path = modelPath + "/" + m._1
      val model = m._2.asInstanceOf[Saveable]
      model.save(ss.sparkContext, path)
    })

    //写出ML Model
    writeableModelMap.foreach(m => {
      val path = modelPath + "/" + m._1
      val model = m._2.asInstanceOf[MLWritable]
      model.save(path)
    })

    true
  }

  protected def modelContentLoadInner(modelPath: String): Boolean = {
    implicit val formats = Serialization.formats(NoTypeHints)

    val valuePath = modelPath + Constants.MODEL_PARAM_FILE
    val valueMap =
      if (HdfsUtil.IsDirExist(valuePath)) {
        Map[String, String]()
        //todo 读取参数当前只支持字符串类型
        val fields = this.getClass.getDeclaredFields
            .filter(f => !f.getName.equals("modelName"))
            .filter(f => f.getType != classOf[Logger])
            .map(_.getName)
          val value = HdfsUtil.getHDFSFileContent(valuePath)
          val valueStringMap = read[Map[String, String]](value).filter(a => fields.contains(a._1))
          valueStringMap
      } else {
        Map[String, String]()
      }
    val ss: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()

    this.getClass.getDeclaredFields.foreach(f => {
      f.setAccessible(true)
      if (f.getType == classOf[DataFrame]) {   //读取dataframe
        val path = modelPath + f.getName
        f.set(this, ss.read.parquet(path))
      } else if (classOf[Saveable].isAssignableFrom(f.getType)) {   //读取MLlib Model
        val path = modelPath + f.getName
        val model = f.getType.getDeclaredMethod("load", classOf[SparkContext], classOf[String])
          .invoke(null, ss.sparkContext, path)
        f.set(this, model)
      }else if (classOf[MLWritable].isAssignableFrom(f.getType)) {  //读取ML Model
        // todo 未测试
        val path = modelPath + f.getName
        val model = f.getType.getDeclaredMethod("load", classOf[String]).invoke(null, path)
        f.set(this, model)
      } else {     //读取基本数据类型参数
        val value = valueMap.getOrElse(f.getName, null)
        if (value != null && f.getType == classOf[String]) {
          f.set(this, value)
        }
      }
    })
    true
  }


  /**
    * 读取model的指定属性
    *
    * @param dataKey 属性名
    * @return 不存在返回null
    */
  private def getData(dataKey: String): DataFrame = {

    val field = this.getClass.getDeclaredField(dataKey)
    field.setAccessible(true)
    val data = field.get(this)

    //    val run = runtimeMirror(getClass.getClassLoader)
    //    val symbol = getType(this).decl(TermName(dataKey))
    //    if (symbol.equals(NoSymbol)) {
    //      return null
    //    }
    //    val data = run.reflect(this).reflectField(symbol.asTerm).get
    data match {
      case r: DataFrame =>
        r
      case _ =>
        null
    }
  }

  private def getType[T: TypeTag](obj: T) = typeOf[T]

  private def getModelPathBase(env: EnvEnum.Value): String = {
    env match {
      case EnvEnum.PRO => Constants.MODEL_PATH_BASE
      case EnvEnum.TEST => Constants.MODEL_PATH_BASE_DEBUG + EnvEnum.TEST.toString.toLowerCase + "/"
      case other: EnvEnum.Value => Constants.MODEL_PATH_BASE_DEBUG + other.toString.toLowerCase + "/"
      case _ => throw new RuntimeException("不合法的环境类型")
    }
  }

  private def getOutputPathBase(env: EnvEnum.Value): String = {
    env match {
      case EnvEnum.PRO => Constants.OUTPUT_PATH_BASE
      case EnvEnum.TEST => Constants.OUTPUT_PATH_BASE_DEBUG + EnvEnum.TEST.toString.toLowerCase + "/"
      case other: EnvEnum.Value => Constants.OUTPUT_PATH_BASE_DEBUG + other.toString.toLowerCase + "/"
      case _ => throw new RuntimeException("不合法的环境类型")
    }
  }


}

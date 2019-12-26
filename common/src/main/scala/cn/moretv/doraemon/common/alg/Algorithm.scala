package cn.moretv.doraemon.common.alg

import cn.moretv.doraemon.common.path.HdfsPath
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

/**
  * Created by lituo on 2018/6/12.
  */
trait Algorithm extends Serializable{

  protected val algParameters: AlgParameters
  protected val modelOutput: Model
  protected var dataInput: Map[String, DataFrame] = _

  private val logger = LoggerFactory.getLogger(this.getClass)

  final def getParameters: AlgParameters = algParameters

  final def initParameter(hdfsPath: HdfsPath): Boolean = {
    algParameters.loadFromHdfs(hdfsPath)
  }

  final def initInputData(dataMap: Map[String, DataFrame]): Unit = {
    dataInput = dataMap
  }

  final def run(): Unit = {
    val className = this.getClass.getSimpleName
    logger.info(s"=====Parameter Validation Start [$className]====")
    paramValidation()
    logger.info(s"=====Before Invoke Start [$className]====")
    beforeInvoke()
    logger.info(s"=====Invoke Start [$className]====")
    invoke()
    logger.info(s"=====After Invoke Start [$className]====")
    afterInvoke()
    logger.info(s"=====Run Method Finish [$className]====")
  }

  //获取最终结果的Model
  final def getOutputModel: Model = modelOutput

  final def paramValidation(): Unit = {
    val r: String = algParameters.validation()
    if(r != null && r.trim.isEmpty && !"ok".equalsIgnoreCase(r)) {
      throw new IllegalArgumentException(algParameters.algName + "参数校验不合法：" + r)
    }
  }

  protected def beforeInvoke(): Unit

  protected def invoke(): Unit

  protected def afterInvoke(): Unit

}

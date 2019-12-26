package cn.moretv.doraemon.common.data

import cn.moretv.doraemon.common.path.Path
import org.apache.spark.sql.DataFrame
import scala.reflect.runtime.universe

/**
  * Created by lituo on 2018/6/13.
  */
object DataReader {
  private val runtimeMirror: universe.Mirror = scala.reflect.runtime.universe.runtimeMirror(getClass.getClassLoader)

  def read(path: Path): DataFrame = {
    val className = "cn.moretv.doraemon.data.reader.DataReader"
    val dataReader = getMethod(className, "read")
    dataReader(path).asInstanceOf[DataFrame]
  }

  private def getMethod(className: String, methodName: String): universe.MethodMirror = {
    val classInstance = Class.forName(className).newInstance()
    val instanceMirror = runtimeMirror.reflect(classInstance)
    //    val classMirror = runtimeMirror.reflectClass(classSymbol)

    val classSymbol = runtimeMirror.staticClass(className)
    val methodSymbol = classSymbol.selfType.decl(scala.reflect.runtime.universe.TermName(methodName)).asMethod

    instanceMirror.reflectMethod(methodSymbol)
  }
}

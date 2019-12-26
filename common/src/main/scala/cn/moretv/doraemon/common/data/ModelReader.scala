package cn.moretv.doraemon.common.data

import cn.moretv.doraemon.common.alg.Model
import cn.moretv.doraemon.common.constant.Constants
import cn.moretv.doraemon.common.path.{HdfsPath, Path}
import cn.moretv.doraemon.common.util.HdfsUtil


/**
  * Created by lituo on 2018/6/13.
  */
object ModelReader {

  def read(path: Path): Model = {
    path match {
      case modelPath: HdfsPath => {
        val modelInfoPath = modelPath.getHdfsPath() + Constants.MODEL_INFO_FILE
        val className = HdfsUtil.getHDFSFileContent(modelInfoPath)
        if (className == null || className.isEmpty) {
          throw new IllegalArgumentException("存储的路径不是model")
        }
        try {
          val model = Class.forName(className).newInstance().asInstanceOf[Model]
          model.load(modelPath)
          model
        } catch {
          case _: ClassNotFoundException =>
            throw new IllegalArgumentException("未找到model的定义：" + className)
        }
      }
      case _ => throw new IllegalArgumentException("不支持的参数类型")
    }
  }

}

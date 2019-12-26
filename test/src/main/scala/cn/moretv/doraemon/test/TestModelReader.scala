package cn.moretv.doraemon.test

import cn.moretv.doraemon.algorithm.testalg.TestAlgModel
import cn.moretv.doraemon.common.data.ModelReader
import cn.moretv.doraemon.common.path.HdfsPath

/**
  * Created by lituo on 2018/6/15.
  */
object TestModelReader extends BaseClass {

  override def execute(): Unit = {
    val model = ModelReader.read(new HdfsPath("/tmp/model/test_alg/test/"))
    model.asInstanceOf[TestAlgModel].data.show()
  }
}

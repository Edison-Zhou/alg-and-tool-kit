package cn.moretv.doraemon.common.path

/**
  * Created by lituo on 2018/6/29.
  */
case class KeyedPath(
                      /**
                        * 预定义的路径配置名，对应hdfs上的文件名
                        */
                      key: String,

                      /**
                        * 对指定的某条配置进行覆盖
                        */
                      paramMap: Map[String, Any],

                      /**
                        * 配置中存在占位符的具体值
                        */
                      placeholderValueMap: Map[String, String]
                    ) extends Path {

}

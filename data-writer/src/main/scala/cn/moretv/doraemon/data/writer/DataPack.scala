package cn.moretv.doraemon.data.writer


import cn.moretv.doraemon.common.enum.FormatTypeEnum
import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.mutable


/**
  * Created by lituo on 2018/6/26.
  */
object DataPack {

  //implicit val formats = Serialization.formats(NoTypeHints)

  def pack(df: DataFrame, param: DataPackParam): DataFrame = {

    if (param == null) {
      throw new IllegalArgumentException("参数不能是null")
    }

    //数据验证
    param.format match {
      case FormatTypeEnum.KV =>

      case FormatTypeEnum.ZSET =>
        if (df.schema.fields.length != 2) {
          throw new RuntimeException("zset的格式的数据必须是两列")
        }
        if (!df.schema.fields(1).dataType.isInstanceOf[ArrayType]) {
          throw new RuntimeException("zset第二列的格式必须是array")
        }

      case FormatTypeEnum.HASH =>

      case _ => throw new IllegalArgumentException("不能支持的格式" + param.format)
    }

    var outputDf = df

    //添加额外的列
    if (param.extraValueMap != null) {
      param.extraValueMap.foreach(entry => {
        outputDf = outputDf.withColumn(entry._1, lit(entry._2))
      })
    }

    val keyColumnName = outputDf.schema.fields(0).name

    //添加key前缀
    if (param.keyPrefix != null && param.keyPrefix.nonEmpty) {
      outputDf = outputDf.withColumn(keyColumnName, expr("concat('" + param.keyPrefix + "', " + keyColumnName + ")"))
    }

    val valueColumnNames = outputDf.schema.fields.map(_.name).filter(_ != keyColumnName)

    val spark: SparkSession = SparkSession.builder().getOrCreate()
    import spark.implicits._

    //数据字段处理和序列化
    param.format match {
      case FormatTypeEnum.KV | FormatTypeEnum.HASH =>
        outputDf.rdd.map(row => {
          val key = row.getAs[String](keyColumnName)
          val valueMap = row.getValuesMap[Any](valueColumnNames)
          //如果是json格式的string首先反序列化，仅支持一层
          val jsonValueMap = valueMap.mapValues[Any] {
            case str: String =>
              if (str.startsWith("{") || str.startsWith("[")) {
                try {
                  JSON.parse(str)
                } catch {
                  case e: Exception => {
                    throw new RuntimeException(str,e)
                  }
                }
              } else {
                str
              }
            case arr:Seq[Object] =>
              arr.toArray
            case value => value
          }.map(e => (e._1,e._2.asInstanceOf[Object]))
          val jsonObject = new JSONObject(jsonValueMap)
          (key, jsonObject.toString)
        }).toDF("key", "value")

      case FormatTypeEnum.ZSET =>
        outputDf.rdd.map(row => {
          val key = row.getAs(0).toString
          val value = row.getList(1)
          val map = new mutable.HashMap[String, Double]
          value.toArray().foreach(v => {
            val pair = v.asInstanceOf[GenericRowWithSchema]
            val item = pair.get(0).toString
            val score = pair.getDouble(1)
            map.put(item, score)
          })
          if(param.zsetAlg != null && param.zsetAlg.nonEmpty) {
            map.put(param.zsetAlg, map.values.max.ceil + 1000.0)
          }

          (key, map)
        }).toDF("key", "value")
    }

  }
}

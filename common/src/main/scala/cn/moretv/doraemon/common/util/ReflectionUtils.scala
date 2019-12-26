package cn.moretv.doraemon.common.util

import java.util.UUID

/**
  * Created by baozhiwang on 2018/6/8.
  */
object ReflectionUtils {

  /**
    * Returns a random UID that concatenates the given prefix, "_", and 12 random hex chars.
    */
  def randomUID(prefix: String): String = {
    prefix + "_" + UUID.randomUUID().toString.takeRight(12)
  }
}

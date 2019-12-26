package cn.moretv.doraemon.algorithm.util

import org.apache.spark.mllib.linalg.{SparseVector, Vectors}

import scala.collection.mutable.ArrayBuffer

/**
  *
  * @author wang.baozhi 
  * @since 2018/9/27 下午4:56 
  */
object VectorUtils {
  /**
    * 用于将向量转化为稀疏向量
    *
    * @param vector 向量
    * @param util Numeric[T]
    * @tparam T 泛型
    * @return 稀疏向量
    */
  def denseVector2Sparse[T<: AnyVal](vector: Seq[T])(implicit util: Numeric[T]): SparseVector = {
    val size = vector.length
    val sequence = new ArrayBuffer[(Int, Double)]()
    for(i<- 0 until size) {
      val value = util.toDouble(vector(i))
      if(value != 0)
        sequence += ((i, value))
    }

    Vectors.sparse(size, sequence).toSparse
  }

  /**
    * 用于计算两个稀疏向量的余弦相似度
    * @param V1 稀疏向量1
    * @param V2 稀疏向量2
    * @return 余弦相似度
    */
  def cosineSimilarity(V1: SparseVector, V2: SparseVector): Double = {
    val value1 = dotProduct(V1, V2)
    val value2 = math.sqrt(dotProduct(V1, V1))
    val value3 = math.sqrt(dotProduct(V2, V2))
    if(value2 == 0 || value3 == 0)
      0
    else
      value1/(value2 * value3)
  }

  /**
    * 用于计算两个稀疏向量的内积
    * @param V1 稀疏向量1
    * @param V2 稀疏向量2
    * @return 内积
    */
  def dotProduct(V1: SparseVector, V2: SparseVector):Double = {
    var result:Double = 0.0

    var i = 0
    var j = 0
    val length1 = V1.indices.length
    val length2 = V2.indices.length
    while(i < length1 && j < length2) {
      if(V1.indices(i) < V2.indices(j)) {
        i = i + 1
      }
      else if(V1.indices(i) > V2.indices(j)) {
        j = j + 1
      }
      else {
        result = result + V1.values(i) * V2.values(j)
        i = i + 1
        j = j + 1
      }
    }

    result
  }
}

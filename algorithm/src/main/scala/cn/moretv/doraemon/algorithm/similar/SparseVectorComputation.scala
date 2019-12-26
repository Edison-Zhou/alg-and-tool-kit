package cn.moretv.doraemon.algorithm.similar

import breeze.optimize.linear.PowerMethod.BDV
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vectors}

import scala.collection.mutable.ArrayBuffer

object SparseVectorComputation extends Serializable{

   /**
    * 用于将向量转化为稀疏向量
    * @param vector 向量
    * @param util Numeric[T]
    * @tparam T 泛型
    * @return 稀疏向量
    */
  def denseVector2Sparse[T<:AnyVal](vector: Seq[T])(implicit util:Numeric[T]):SparseVector = {
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
    * 用于计算两个稀疏向量的和(稀疏向量indices已序列化)
    * @param V1 稀疏向量1
    * @param V2 稀疏向量2
    * @return 稀疏向量
    */
  def vectorPlus(V1:SparseVector, V2:SparseVector):SparseVector = {
    require(V1.size == V2.size, "The two SparseVector have different sizes!")
    val size = V1.size
    val sequence = new ArrayBuffer[(Int, Double)]()

    var i = 0
    var j = 0
    val length1 = V1.indices.length
    val length2 = V2.indices.length

    while(i < length1 && j < length2) {
      if(V1.indices(i) < V2.indices(j)) {
        sequence += ((V1.indices(i), V1.values(i)))
        i = i + 1
      }
      else if(V1.indices(i) > V2.indices(j)) {
        sequence += ((V2.indices(j), V2.values(j)))
        j = j + 1
      }
      else {
        sequence += ((V1.indices(i), V1.values(i) + V2.values(j)))
        i = i + 1
        j = j + 1
      }
    }

    while(i < length1) {
      sequence += ((V1.indices(i), V1.values(i)))
      i = i + 1
    }

    while(j < length2) {
      sequence += ((V2.indices(j), V2.values(j)))
      j = j + 1
    }

    Vectors.sparse(size, sequence).toSparse
  }

  /**
    * 用于计算两个稀疏向量的差(稀疏向量indices已序列化)
    * @param V1 稀疏向量1
    * @param V2 稀疏向量2
    * @return 稀疏向量
    */
  def vectorSubtract(V1:SparseVector, V2:SparseVector):SparseVector = {
    require(V1.size == V2.size, "The two SparseVector have different sizes!")
    val size = V1.size
    val sequence = new ArrayBuffer[(Int, Double)]()

    var i = 0
    var j = 0
    val length1 = V1.indices.length
    val length2 = V2.indices.length

    while(i < length1 && j < length2) {
      if(V1.indices(i) < V2.indices(j)) {
        sequence += ((V1.indices(i), V1.values(i)))
        i = i + 1
      }
      else if(V1.indices(i) > V2.indices(j)) {
        sequence += ((V2.indices(j), -V2.values(j)))
        j = j + 1
      }
      else {
        sequence += ((V1.indices(i), V1.values(i) - V2.values(j)))
        i = i + 1
        j = j + 1
      }
    }

    while(i < length1) {
      sequence += ((V1.indices(i), V1.values(i)))
      i = i + 1
    }

    while(j < length2) {
      sequence += ((V2.indices(j), -V2.values(j)))
      j = j + 1
    }

    Vectors.sparse(size, sequence).toSparse
  }

  /**
    * 用于计算稀疏向量与数值的乘积
    * @param V 稀疏向量
    * @param number 数值
    * @param util Numeric[T]
    * @tparam T 泛型
    * @return 稀疏向量
    */
  def vectorTimes[T<:AnyVal](V:SparseVector, number:T)(implicit util:Numeric[T]):SparseVector = {
    Vectors.sparse(V.size, V.indices, V.values.map(e => e * util.toDouble(number))).toSparse
  }


  /**
    * 用于计算两个稀疏向量的内积
    * @param V1 稀疏向量1
    * @param V2 稀疏向量2
    * @return 内积
    */
    def vectorDotProduct(V1:SparseVector, V2:SparseVector):Double = {
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

  /**
    * 用于计算两个稀疏向量的加权內积
    * @param V1 稀疏向量1
    * @param V2 稀疏向量2
    * @param weightVector 权重向量
    * @return Double
    */
  def vectorDotProductWeight(V1:SparseVector, V2:SparseVector, weightVector:SparseVector):Double = {
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
        val index = weightVector.indices.indexOf(V1.indices(i))
        var weight = 0.0
        if(index != -1)
          weight = weightVector.values(index)

        result = result + V1.values(i) * V2.values(j) * weight
        i = i + 1
        j = j + 1
      }
    }

    result
  }

  /**
    * 用于计算两个稀疏向量的余弦相似度
    * @param V1 稀疏向量1
    * @param V2 稀疏向量2
    * @return 余弦相似度
    */
  def vectorCosineSimilarity(V1:SparseVector, V2:SparseVector):Double = {
    val value1 = vectorDotProduct(V1, V2)
    val value2 = math.sqrt(vectorDotProduct(V1, V1))
    val value3 = math.sqrt(vectorDotProduct(V2, V2))
    if(value2 == 0 || value3 == 0)
      0
    else
      value1/(value2 * value3)
  }

  def vectorCosineSimilarity2(V1:DenseVector, V2:DenseVector):Double = {
    val BV1 = new BDV(V1.values)
    val BV2 = new BDV(V2.values)
    val value1 = BV1.dot(BV2)
    val value2 = math.sqrt(BV1.dot(BV1))
    val value3 = math.sqrt(BV2.dot(BV2))
    if(value2 == 0 || value3 == 0)
      0
    else
      value1/(value2 * value3)
  }

  /**
    * 用于计算两个稀疏向量的加权余弦相似度
    * @param V1 稀疏向量1
    * @param V2 稀疏向量2
    * @param weightVector 权重向量
    * @return 加权余弦相似度
    */
  def vectorCosineSimilarityWeight(V1:SparseVector, V2:SparseVector, weightVector:SparseVector):Double = {
    val value1 = vectorDotProductWeight(V1, V2, weightVector)
    val value2 = math.sqrt(vectorDotProductWeight(V1, V1, weightVector))
    val value3 = math.sqrt(vectorDotProductWeight(V2, V2, weightVector))
    if(value2 == 0 || value3 == 0)
      0
    else
      value1/(value2 * value3)
  }
}

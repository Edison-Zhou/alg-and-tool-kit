package cn.moretv.doraemon.algorithm.similar


/**
  * 向量计算
  *
  * @author zhangnan
  */
trait VectorComputation[T] {
  /**
   * 加法
   * @return
   */
  def vectorPlus[T](V1:T, V2:T):T

  /**
    * 减法
    * @return
    */
  def vectorSubtract[T](V1:T, V2:T):T


  /**
    * 乘法
    * @return
    */
  def vectorTimes[T](V1:T, V2:T):T

  /**
    * 点积
    * @return
    */
  def vectorDotProduct[T](V1:T, V2:T):Double


  /**
    * 加权点积
    * @return
    */
   def vectorDotProductWeight[T](V1:T, V2:T,weight:T):Double


  /**
    * Cos距离
    * @return
    */
  def vectorCosineSimilarity[T](V1:T, V2:T):Double


  /**
    * Cos距离 加权
    * @return
    */
  def vectorCosineSimilarityWeight[T](V1:T, V2:T,weight:T):Double

}

package cn.moretv.doraemon.algorithm.cluster

import cn.moretv.doraemon.algorithm.similar.SparseVectorComputation
import cn.moretv.doraemon.common.alg.Algorithm
import org.apache.spark.ml
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{SparseVector, Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * 基于kmeans的定制化聚类
  * Updated by lituo on 2018/7/16
  */
class ClusterKmeansAlgorithm extends Algorithm {
  //数据Map的Key定义

  //输入输出属性定义

  /*
  * 向量的数据
  * */
  val INPUT_DATA_KEY_VECTOR = "vector"

  //输入输出属性定义
  val algParameters: ClusterKmeansParameters = new ClusterKmeansParameters()
  val modelOutput: ClusterKmeansModel = new ClusterKmeansModel()

  //进行参数初始化设置
  override def beforeInvoke(): Unit = {

  }

  override def invoke(): Unit = {
    val ss = SparkSession.builder().getOrCreate()
    val data = dataInput(INPUT_DATA_KEY_VECTOR).persist()
    val multiClusterResult = algParameters.clusterNums.map(k => longVideoCluster(ss, data, k))
      .reduce((a, b) => a.union(b))

    val clusterCenter = getClusterCenter(ss, multiClusterResult)
    val unionClusters = kmeans4clusterUnion(clusterCenter.map(e => (e._1, Vectors.dense(e._2), e._3)), algParameters.clusterUnionNum)
    val unionClusters2RDD = ss.sparkContext.parallelize(unionClusters)

    import ss.implicits._
    modelOutput.matrixData = unionClusters2RDD.toDF("cluster", "items")
  }

  /**
    *
    * @param ss
    * @param featureDf 特征表（sid，vector）
    * @param k         聚类的个数
    * @return RDD[(cluster, Iterable[(sid, similarity, vector)])]
    */
  def longVideoCluster(ss: SparkSession,
                       featureDf: DataFrame,
                       k: Int): RDD[(Int, Seq[(String, Double, Vector)])] = {


    //    val kMeans = new KMeans().setK(k).setMaxIter(100)
    //    val model = kMeans.fit(featureDf)
    //
    //    val videoData = model.transform(featureDf)
    import ss.implicits._

    val kMeans = new KMeans().setK(k).setMaxIterations(100)
    val featureRdd = featureDf.rdd.map(e => (e.getString(0), SparseVector.fromML(e.getAs[ml.linalg.Vector](1).toSparse)))
    val model = kMeans.run(featureRdd.map(e => e._2))

    val videoData = featureRdd.map(e => {
      val sid = e._1
      val feature = e._2
      val classification = model.predict(feature)

      (sid, feature, classification)
    }).toDF()

    //    println("video cluster by dataSet")
    //    videoData.show()
    //    videoData.printSchema()

    computerDistanceAndFilter(videoData, model.clusterCenters)
  }

  /**
    * 计算视频与视频聚类中心点之间的相似度
    *
    * @param videoData （sid, vector, cluster）
    * @param clusters  Array[clusterCenters]
    * @return RDD[(cluster, Iterable[(sid, similarity, vector)])]
    */
  def computerDistanceAndFilter(videoData: DataFrame, clusters: Array[Vector]): RDD[(Int, Seq[(String, Double, Vector)])] = {
    val data: RDD[(Int, Seq[(String, Vector)])] =
      videoData.rdd.map(e => (e.getInt(2), (e.getString(0), e.getAs[Vector](1).toSparse)))
        .groupByKey()
        .map(e => (e._1, e._2.toSeq))

    data.map(e => {
      val classification = e._1
      val classCenter = clusters(classification).toSparse

      val videosBelong2thisClass = e._2.map(s => {
        val sid = s._1
        val feature: Vector = s._2
        val similarity = SparseVectorComputation.vectorCosineSimilarity(classCenter.asML, feature.toSparse.asML)
        (sid, similarity, feature)
      }).filter(s => s._2 > algParameters.filterMinSimilarity)

      (classification, videosBelong2thisClass)
    }).filter(e => e._2.size >= algParameters.filterClusterMinSize)
  }

  /**
    *
    * @param ss
    * @param videoCluster RDD[(cluster, Iterable[(sid, similarity, vector)])]
    * @return RDD[(cluster_index, center:Array[Double], Array[(sid)])]
    */
  def getClusterCenter(ss: SparkSession,
                       videoCluster: RDD[(Int, Seq[(String, Double, Vector)])]
                      ): RDD[(Int, Array[Double], Array[String])] = {

    val videoClusterData = videoCluster.
      map(e => (e._1, e._2.map(x => (x._1, x._3.toDense.values)))).
      map(e => (e._1, e._2.filter(x => x._2.length > 0))).
      filter(e => e._2.length > 0).
      map(e => {
        val clusterIndex = e._1
        val videoSet = e._2.map(x => x._1)
        val videoFeatures = e._2.map(x => x._2)
        val featureLength = videoFeatures.head.length
        val center = new Array[Double](featureLength)
        for (video <- videoFeatures) {
          for (i <- center.indices) {
            center(i) += video(i)
          }
        }
        for (i <- center.indices) {
          center(i) /= videoSet.length
        }
        (clusterIndex, center, videoSet.toArray)
      })

    videoClusterData
  }

  /**
    * 对聚类结果进行二次聚类，以便合并非常雷同或接近的类别
    *
    * @param dataSet 初次聚类结果
    * @param k       聚类个数
    * @return Array[(cluster_index, Array[sid])]
    */
  def kmeans4clusterUnion(dataSet: RDD[(Int, Vector, Array[String])],
                          k: Int): Array[(Int, Array[String])] = {

    println("dataSet")
    //    println(dataSet.count())

    val kMeans = new KMeans().setK(k).setMaxIterations(100)
    val model = kMeans.run(dataSet.map(e => e._2))

    val clusterData = dataSet.map(e => {
      val clusterIndex = e._1
      val feature = e._2
      val videoSet = e._3
      val classification = model.predict(feature)

      (clusterIndex, feature, classification, videoSet)
    })
    //    println("video clusters")
    //    println(clusterData.count())

    val totalVideos = dataSet.map(e => e._3).collect().flatMap(e => e.map(x => x))
    //    println("total videos")
    //    println(totalVideos.size)

    val videoClasses = computerDistance4movieCluster(clusterData, model.clusterCenters)

    val filterClasses = videoClasses.collect().filter(e => e._2.size >= 1)

    //    println("videoClasses")
    //    println(videoClasses.count())

    //    println("filterClasses")
    //    println(filterClasses.size)

    val coveredMovies = filterClasses.flatMap(e => e._2.flatMap(x => x._3)).distinct
    println("coveredMovies")
    println(coveredMovies.size)


    val unionClasses = videoClasses.map(e => (e._1, e._2.map(x => x._3))).map(e => {
      var result = new Array[String](0)
      e._2.foreach(x => result = result ++: x)
      (e._1, result.distinct)
    }).map(e => e._2).collect()

    unionClasses.map(e => {
      val clusterIndex = unionClasses.indexOf(e)
      (clusterIndex, e)
    })
  }

  /**
    * 计算聚类中心点之间的相似度，用于对聚类结果进行合并
    *
    * @param clusterData   RDD[(cluster_index, Vector, Int, Array[(sid])]
    * @param modelClusters Array[Vector]
    * @return RDD[(Int, Iterable[(cluster_index, similarity, Array[sid])])]
    */
  def computerDistance4movieCluster(
                                     clusterData: RDD[(Int, Vector, Int, Array[String])],
                                     modelClusters: Array[Vector]
                                   ): RDD[(Int, Iterable[(Int, Double, Array[String])])] = {
    val data = clusterData.map(e => (e._3, (e._1, e._2, e._4))).groupByKey()

    data.map(e => {
      val classification = e._1
      val classCenter = modelClusters(classification).toSparse

      val videoClusterBelong2thisClass = e._2.map(x => {
        val cluster = x._1
        val vector = x._2
        val videoSet = x._3
        val similarity = SparseVectorComputation.vectorCosineSimilarity(classCenter.asML, vector.toSparse.asML)
        (cluster, similarity, videoSet)
      })

      (classification, videoClusterBelong2thisClass)
    })
  }

  //清理阶段
  override def afterInvoke(): Unit = {
  }
}

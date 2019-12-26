package cn.moretv.doraemon.test.als

import java.io.File

import cn.moretv.doraemon.algorithm.similar.latest.{SimilarLatestParameter, SimilarLatestAlgorithm}

import cn.moretv.doraemon.common.path.HdfsPath
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.test.BaseClass
import cn.moretv.doraemon.test.constant.PathConstants
import cn.moretv.doraemon.test.util.BizUtils
/**
  *
  * @author wang.baozhi 
  * @since 2018/7/9 下午7:38
  *
  *        用于给用户推荐看过的最后一部电影的相似内容
  *        去掉了同质电影的映射关系
  */
object SimilarityRecommend extends BaseClass {
  //生产路径 val pathOfMoretvSimilarityRecommend = "/ai/data/dw/moretv/SimilartityRecommend"
  //临时路径
  val pathOfMoretvSimilarityRecommend = "/ai/data/dw/moretv_tmp"+File.separator+"SimilartityRecommend"

  override def execute(): Unit = {
    //1.获取用户观看的最后一部电影
    val userWatchedLastMovies = DataReader.read(new HdfsPath(PathConstants.pathOfMoretvMovieScore))

    //2.获取相似影片数据
    val similarMovies = BizUtils.getDataFrameNewest(PathConstants.pathOfMoretvSimilarMovie)

    //3.获取用户近期看过的电影
     val userWatchedMovies = DataReader.read(new HdfsPath(PathConstants.pathOfMoretvMovieScore))

    //4.获取首页曝光给用户的长视频
    val frontPageExposureLongVideos = BizUtils.getDataFrameWithDataRange(PathConstants.pathOfMoretvFrontPageExposureLongVideos,
      90)

    //算法部分
    val similarLatestAlgorithm = new SimilarLatestAlgorithm
    val longVideoClusterParameters = similarLatestAlgorithm.getParameters.asInstanceOf[SimilarLatestParameter]
    longVideoClusterParameters.numOfDaysRetainAnImpression = 1
    longVideoClusterParameters.numOfDaysUserWatchedLongVideos = 300
    longVideoClusterParameters.thresholdScoreOfUserPrefer = 0.5
    val similarLatestDataMap = Map(similarLatestAlgorithm.INPUT_USER_WATCHED_LAST_MOVIES->userWatchedLastMovies,
      similarLatestAlgorithm.INPUT_SIMILAR_MOVIE -> similarMovies,
      similarLatestAlgorithm.INPUT_USER_WATCHED_MOVIES -> userWatchedMovies,
      similarLatestAlgorithm.INPUT_FRONT_PAGE_EXPOSED_LONG_VIDEOS -> frontPageExposureLongVideos)

    similarLatestAlgorithm.initInputData(similarLatestDataMap)
    similarLatestAlgorithm.run()
//    similarLatestAlgorithm.getOutputModel.save(new HdfsPath(pathOfMoretvSimilarityRecommend))
  }

}

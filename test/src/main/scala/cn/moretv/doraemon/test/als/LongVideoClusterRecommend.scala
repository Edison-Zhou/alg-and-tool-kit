package cn.moretv.doraemon.test.als

import cn.moretv.doraemon.algorithm.longVideoCluster.{LongVideoClusterModel, LongVideoClusterAlgorithm, LongVideoClusterParameters}
import cn.moretv.doraemon.common.path.HdfsPath
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.test.constant.PathConstants
import cn.moretv.doraemon.test.util.BizUtils
import cn.moretv.doraemon.test.{BaseClass, Constants}

/**
  *
  * 使用用户长视频聚类的特征，使用曝光日志过滤掉用户已经看过的长视频
  * 去掉了地域屏蔽和同质电影的映射关系替换
  *
  * @author wang.baozhi 
  * @since 2018/7/13 下午2:32
  */
object LongVideoClusterRecommend extends BaseClass {
 //生产环境输出路径 val pathOfLongVideoClusterRecommend = "/ai/data/dw/moretv/longVideoClusterRecommend"
  //临时输出路径
  val pathOfLongVideoClusterRecommend = PathConstants.tmp_long_video_cluster_dir

  override def execute(): Unit = {
    try {
      //1.获取用户电影聚类的特征
      val userFeaturesByLongVideoCluster = BizUtils.getDataFrameNewest(PathConstants.pathOfMoretvUserLongVideoClusterFeatures)
      //2.获取首页曝光给用户的长视频
      val frontPageExposureLongVideos = BizUtils.getDataFrameWithDataRange(PathConstants.pathOfMoretvFrontPageExposureLongVideos,
        90)
      //3.获取长视频聚类信息
      val longVideoClusterData = BizUtils.getDataFrameNewest(PathConstants.pathOfLongVideoClusters)
      //4.获取有效视频
      val validLongVideo = BizUtils.getValidLongVideoSid
      //5.获取用户看过的长视频
      val userWatchedLongVideos = DataReader.read(new HdfsPath(PathConstants.pathOfMoretvLongVideoScore))
      //算法部分
      val longVideoClusterAlgorithm = new LongVideoClusterAlgorithm
      val longVideoClusterParameters = longVideoClusterAlgorithm.getParameters.asInstanceOf[LongVideoClusterParameters]
      longVideoClusterParameters.numOfPastBasedLongVideoCluster4Filter = 80
      longVideoClusterParameters.numOfLongVideoCluster4Recommend = 50
      longVideoClusterParameters.numOfDaysUserWatchedLongVideos = 300
      val longVideoClusterDataMap = Map(longVideoClusterAlgorithm.INPUT_USER_FEATURES_BY_LONG_VIDEO_CLUSTER -> userFeaturesByLongVideoCluster,
        longVideoClusterAlgorithm.INPUT_FRONT_PAGE_EXPOSED_LONG_VIDEOS -> frontPageExposureLongVideos,
        longVideoClusterAlgorithm.INPUT_LONG_VIDEO_CLUSTER_DATA -> longVideoClusterData,
        longVideoClusterAlgorithm.INPUT_VALID_LONG_VIDEO -> validLongVideo, longVideoClusterAlgorithm.INPUT_USER_WATCHED_LONG_VIDEOS -> userWatchedLongVideos)

      longVideoClusterAlgorithm.initInputData(longVideoClusterDataMap)
      longVideoClusterAlgorithm.run()
//      longVideoClusterAlgorithm.getOutputModel.save(new HdfsPath(pathOfLongVideoClusterRecommend))
    } catch {
      case e: Exception => {
        throw e
      }
    }
  }
}

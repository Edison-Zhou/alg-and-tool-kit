package cn.moretv.doraemon.test.constant

import java.io.File

import cn.moretv.doraemon.test.Constants

/**
  *
  *  用于生产环境,整理后的的路径
  * Created by baozhiwang on 2018/6/20.
  */

object PathConstants {

  //LongVideoClusterRecommend  used
  val pathOfMoretvUserLongVideoClusterFeatures = "/ai/dw/moretv/base/word2vec/user/longVideoClusterFeature"
  val pathOfMoretvFrontPageExposureLongVideos = "/user/hive/warehouse/ai.db/dw_base_behavior_display/product_line=moretv/biz=portalRecommend_ex/key_time=#{date}"
  val pathOfLongVideoClusters = "/ai/data/dw/moretv/videoFeatures/longVideoClusters"
  val pathOfMoretvLongVideoScore = "/user/hive/warehouse/ai.db/dw_base_behavior/product_line=moretv/partition_tag=V1/content_type={movie,tv,zongyi,jilu,comic,kids}/*"

  //SimilarityRecommend used
  val pathOfMoretvMovieScore = "/user/hive/warehouse/ai.db/dw_base_behavior/product_line=moretv/partition_tag=V1/content_type=movie/*"
  val pathOfMoretvSimilarMovie = "/ai/dws/moretv/biz/detailPage/similar/mixedTagAndCluster/movie"

  //FrontPageRecommend used
  //追剧暂时用原来的计算结果
  val pathOfMoretvSeriesChasingRecommend = "/ai/data/dw/moretv/seriesChasingRecommend"
  //byContentType暂时用原来的计算结果,应该使用ALSRecommend的计算结果来代替
  val pathOfMoretvALSResultByContentType = "/ai/data/dw/moretv/ALSResult/byContentType"
  val pathOfSelectMovies = "/ai/dw/moretv/similar/default/movie/defaultSimilarMovies.csv"

  //ALSRecommend used
  val weightOfSelectMovies = 1.1
  val pathOfMoretvLongVideoHistory = "/user/hive/warehouse/ai.db/dw_base_behavior/product_line=moretv/partition_tag=longVideoHistory/content_type={movie,tv,zongyi,jilu,comic,kids}/*"

  //SeriesChasingRecommend used
  val pathOfMoretvSeriesScore = s"/user/hive/warehouse/ai.db/dw_base_behavior_raw_episode/product_line=moretv/score_source=play/partition_tag=#{date}"
  val pathOfMoretvProgram = "/data_warehouse/dw_dimensions/dim_medusa_program"

  //ALS模型生成使用
  val pathOfMoretvActiveUser = "/ai/data/dw/moretv/userBehavior/activeUser"



  //--------------------临时输出目录--------------------在doraemon项目中读写使用，并行运行使用
  //临时测试,输出的基础目录
  val tmp_base_dir = "/ai/data/dw/moretv_tmp"

  //临时测试，ALS模型的输出目录.生产环境真实路径为：/ai/dw/moretv/base/als
  val tmp_als_model_dir = s"$tmp_base_dir"+File.separator+"ALS"

  //临时测试，Similarity输出目录
  val tmp_similarity_dir = s"$tmp_base_dir"+File.separator+"SimilartityRecommend"

  //临时测试，Long Video Cluster Recommend输出路径
  val tmp_long_video_cluster_dir = s"$tmp_base_dir"+File.separator+"longVideoClusterRecommend"

  //首页今日推荐&&会员看看
  //临时测试,生产环境为/ai/dws/moretv/biz/homePage/rec 输出路径
  val tmp_front_page_recommend= s"$tmp_base_dir"+File.separator+"homePage/rec"
  //临时测试,生产环境为/ai/dws/moretv/biz/homePage/vip 输出路径
  val tmp_front_page_recommend_vip= s"$tmp_base_dir"+File.separator+"homePage/vip"

  //ALSRecommend 首页今日推荐的基础数据
  val tmp_als_recommend_dir = s"$tmp_base_dir"+File.separator+"ALS/timeDividedALSResult"
  //ALSRecommend 首页会员看看的基础数据
  val tmp_als_vip_recommend_dir = s"$tmp_base_dir"+File.separator+"ALSResult/byContentType/vip"

  //追剧
  val tmp_series_chasing_dir = s"$tmp_base_dir"+File.separator+"seriesChasing"

}

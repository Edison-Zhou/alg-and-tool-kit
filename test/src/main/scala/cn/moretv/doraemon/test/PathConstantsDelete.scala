package cn.moretv.doraemon.test

/**
  * Created by baozhiwang on 2018/6/20.
  */

/** @deprecated */
object PathConstantsDelete {}
 ///
 ////* val pathOfMoretvUserDailyOperation = "/ai/data/dw/moretv/userBehavior/userDailyOperation/"
 /// val pathOfMoretvActiveUser = "/ai/data/dw/moretv/userBehavior/activeUser"
 /// val pathOfMoretvValuableDeadUser = "/ai/data/dw/moretv/userBehavior/valuableDeadUser"
 /// //val pathOfMoretvVideoUnknownDegree = "/ai/data/dw/moretv/videoFeatures/videoUnknownDegree"
 /// val pathOfMoretvALSResult = "/ai/data/dw/moretv/rec/als/fullAmount"
 /// val pathOfMoretvALSResultByContentType = "/ai/data/dw/moretv/ALSResult/byContentType"
 /// val pathOfMoretvSimilarMovieMap = "/ai/dws/moretv/biz/detailPage/similar/mixedTagAndCluster/movie"
 ///
 /// val pathOfMoretvFrontPageALSResult = "/ai/data/dw/moretv/ALS/frontPageALSResult"
 /// val pathOfMoretvTimeDividedALSResult = "/ai/data/dw/moretv/ALS/timeDividedALSResult"
 /// val pathOfMoretvFrontPagePastRecommendations = "/ai/data/dw/moretv/frontPage/pastRecommendations"
 /// val pathOfMoretvFrontPageALSRecommend = "/ai/data/dw/moretv/ALS/frontPageALSRecommend"
 /// val pathOfMoretvTimeDividedALSRecommend = "/ai/data/dw/moretv/ALS/timeDividedALSRecommend"
 /// val pathOfMoretvFrontPageALSPersonal = "/ai/data/dw/moretv/ALS/frontPagePersonal"
 /// val pathOfMoretvUserPreferredSelectMovies = "/ai/project/hamlet/output/moretv/frontPageMovie/userPreferredMovies"
 /// val pathOfMoretvUserPreferredSelectMoviesRecommend =  "/ai/data/dw/moretv/userPreferredSelectMoviesRecommend"
 /// val pathOfMoretvUserHighlyScoredSingleMovieRecommend = "/ai/data/dw/moretv/userHighlyScoredSingleMovieRecommend"
 /// val pathOfMoretvPastBasedUserHighlyScoredSingleMovies = "/ai/data/dw/moretv/pastBasedUserHighlyScoredSingleMovies"
 ///
 /// /**
 ///   * moretv FrontPage参数
 ///   */
 /// //val pathOfMoretvFrontPageExposureLongVideos = "/user/hive/warehouse/ai.db/dw_base_behavior_display/product_line=moretv/biz=portalRecommend"
 /// val pathOfMoretvFrontPageExposureLongVideos = "/user/hive/warehouse/ai.db/dw_base_behavior_display/product_line=moretv/biz=portalRecommend_ex"
 /// val numOfDays4filterExposureLongVideosInFrontPage = 90
 ///
 /// /**
 ///   * 鲸智3期
 ///   */
 /// val pathOfMoretvALSModel = "/ai/dw/moretv/base/als/model"
 /// val pathOfMoretvUserALSFactors = "/ai/dw/moretv/base/als/user"
 /// val pathOfMoretvVideoALSFactors = "/ai/dw/moretv/base/als/item"
 /// val pathOfMoretvTimeDividedPastRecommendations = "/ai/dws/moretv/biz/homePage/rec"
 /// val pathOfMoretvFrontPageAlgRec = "/ai/dw/moretv/abgroup/homepagerec"
 ///
 /// /** doraemon added for als */
 /// val pathOfMoretvAlsBase = "/ai/dw/moretv/base/als"
 /// val alsUerColName = "user"
 /// val alsItemColName = "item"
 ///
 /// /**
 ///   * MovieClusterRecommend
 ///   */
 /// val pathOfMoretvUserMovieClusterFeatures = "/ai/dw/moretv/base/word2vec/user"
 /// val pathOfMovieClusters = "/ai/data/dw/moretv/videoFeatures/movieClusters"
 /// val pathOfMovieClusterRecommend = "/ai/data/dw/moretv/movieClusterRecommend"
 /// val pathOfMoretvPastBasedMovieClusters = "/ai/data/dw/moretv/pastBasedMovieClusters"
 /// val pathOfMoretvCurrentBasedMovieClusters = "/ai/data/dw/moretv/currentBasedMovieClusters"
 ///
 /// val numOfMovieCluster4Recommend = 50
 /// val numOfPastBasedMovieCluster4Filter = 80
 ///
 /// /**
 ///   * LongVideoClusterRecommend
 ///   */
 /// val pathOfMoretvUserLongVideoClusterFeatures = "/ai/dw/moretv/base/word2vec/user"
 /// val pathOfLongVideoClusters = "/ai/data/dw/moretv/videoFeatures/longVideoClusters"
 /// val pathOfLongVideoClusterRecommend = "/ai/data/dw/moretv/longVideoClusterRecommend"
 ///
 /// val numOfLongVideoCluster4Recommend = 50
 /// val numOfPastBasedLongVideoCluster4Filter = 80
 /// /**
 ///   * SeriesChasingRecommend参数
 ///   */
 /// val pathOfMoretvSeriesScore = s"/user/hive/warehouse/ai.db/dw_base_behavior_raw_episode/product_line=moretv/score_source=play/partition_tag=#{date}"
 /// val pathOfMoretvProgram = "/data_warehouse/dw_dimensions/dim_medusa_program"
 /// val pathOfMoretvSeriesChasingRecommend = "/ai/data/dw/moretv/seriesChasingRecommend"
 /// val thresholdNumOfDaysContinuouslyWatched = 2
 /// val thresholdNumOfIndependentShortStoriesWatched = 3
 ///
 /// val thresholdOfWatch2UpdateRatio = 1.0
 /// val thresholdNumOfIndependentLongStoriesWatched = 2
 ///
 /// /**
 ///   * VIPRecommend参数
 ///   */
 /// val pathOfMoretvPastVipRecommend = "/ai/dws/moretv/biz/homePage/vip"
 ///
 /// /**
 ///   * helios
 ///   */
 /// val pathOfHeliosUserDailyOperation = "/ai/data/dw/helios/userBehavior/userDailyOperation/"
 /// val pathOfHeliosActiveUser = "/ai/data/dw/helios/userBehavior/activeUser"
 /// //val pathOfHeliosVideoUnknownDegree = "/ai/data/dw/helios/videoFeatures/videoUnknownDegree"
 /// val pathOfHeliosALSResult = "/ai/data/dw/helios/fullAmount/ALSResult"
 /// val pathOfHeliosFrontPagePastRecommendations = "/ai/data/dw/helios/frontPage/pastRecommendations"
 /// val pathOfHeliosTimeDividedPastRecommendations = "/ai/data/dw/helios/timeDivided/pastRecommendations"
 /// val pathOfHeliosFrontPageALSRecommend = "/ai/data/dw/helios/ALS/frontPageALSRecommend"
 /// val pathOfHeliosTimeDividedALSRecommend = "/ai/data/dw/helios/ALS/timeDividedALSRecommend"
 ///
 /// /**
 ///   * ALSRecommend 参数
 ///   */
 /// val topNByContentType = 100
 /// val weightOfSelectMovies = 1.1
 ///
 /// /**
 ///   * RecommendUnionAndDistribute参数
 ///   */
 /// val pathOfSelectMovies = "/ai/dw/moretv/similar/default/movie/defaultSimilarMovies.csv"
 ///
 /// /**
 ///   * HDFS读取路径
 ///   * pathOfMoretvScoredData: moretv评分数据路径
 ///   *
 ///   */
 /// //val pathOfMoretvScoredData = "/ai/data/dw/moretv/fullAmountScore/scoreMatrix"
 /// val pathOfMoretvLongVideoScore = "/user/hive/warehouse/ai.db/dw_base_behavior/product_line=moretv/partition_tag=V1/content_type={movie,tv,zongyi,jilu,comic,kids}/*"
 /// val pathOfMoretvLongVideoHistory = "/user/hive/warehouse/ai.db/dw_base_behavior/product_line=moretv/partition_tag=longVideoHistory/content_type={movie,tv,zongyi,jilu,comic,kids}/*"
 /// val pathOfMoretvMovieScore = "/user/hive/warehouse/ai.db/dw_base_behavior/product_line=moretv/partition_tag=V1/content_type=movie/*"
 ///
 ///
 /// //val pathOfHeliosScoredData = "/ai/data/dw/helios/fullAmountScore/scoreMatrix"
 /// val pathOfHeliosLongVideoScore = "/user/hive/warehouse/ai.db/dw_base_behavior/product_line=helios/partition_tag=V1/content_type={movie,tv,zongyi,jilu,comic,kids}/*"
 /// val pathOfHeliosLongVideoHistory = "/user/hive/warehouse/ai.db/dw_base_behavior/product_line=helios/partition_tag=longVideoHistory/content_type={movie,tv,zongyi,jilu,comic,kids}/*"
 ///
 /// /**
 ///   * 风险等级相关
 ///   */
 /// val pathOfCityDangerousLevel = "/data_warehouse/dw_dimensions/dim_medusa_city_dangerous_level"
 /// val pathOfUserCityLevel = "/data_warehouse/user_profile/device_id/medusa/area/default/"
 ///
 /// /**
 ///   * numOfScoreDays: 取多少天的评分数据
 ///   */
 /// val numOfDaysForUserBehavior = 14
 /// val numOfDaysUserWatchedLongVideos = 300
 /// val numOfDaysPastRecommendation = 7
 /// val numOfDaysPastRecommendation_timeDivided = 1
 /// val numOfDaysPastRecommendation_movieCluster = 1
 /// val numOfDaysVideoUnknownDegree = 30
 ///
 /// val numOfPastBasedHighlyScoredSingleMovie = 7
 ///
 /// val numOfALSResult = 550 // 应与ALSRecommend.sh中的参数filterTopN一致
 /// val numOfRecommendSplit2FrontPagePersonal = 150
 ///
 /// val numOfRecommend = 100
 /// val numOfRecommend2Kafka = 50
 /// val numOfRecommend2User = 10
 /// val numOfUserScoredLongVideosThreshold = 1000
 ///
 /// /**
 ///   * Helios参数
 ///   */
 /// val numOfRecommendHelios = 100
 ///
 /// /**
 ///   * SimilarityRecommend所用参数
 ///   * 判断用户喜欢一个节目的最低临界评分
 ///   */
 /// val thresholdScoreOfUserPrefer = 0.5
 /// val thresholdScoreOfUserFavorite = 5  //此参数用户筛选用户特别中意的电影
 /// val thresholdScoreOfSimilarMovies = 0.5
 /// val numOfDaysRetainAnImpression = 1
 /// val pathOfMoretvSimilarityRecommend = "/ai/data/dw/moretv/SimilartityRecommend"
 /// val pathOfMoretvSimilarMoviesEx = "/ai/data/dw/moretv/similarityEx/movie"
 /// val pathOfUserAlgSimilarityRecommend = "/ai/data/dw/moretv/UserAlg/SimilarityRecommend/"
 ///
 /// /**
 ///   * PersonalRecommendation参数
 ///   */
 /// val pathOfMoretvVipUsers = "/data_warehouse/dw_dimensions/dim_medusa_member_right"
 ///
 /// /**
 ///   * 节目库信息
 ///   */
 /// val mySqlDB = "moretv_recommend_mysql"
 /// val tableName = "mtv_program"
 ///
 /// val mySqlDBOfHelios = "helios_recommend_mysql"
 /// val tableNameOfHelios = "mtv_program"
 /// /**
 ///   * 告警邮件信息
 ///   */
 /// val emailsName = Array("cheng.huan@whaley.cn")
 /// */


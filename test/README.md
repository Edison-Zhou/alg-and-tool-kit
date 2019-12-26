# 业务测试模块
依赖于其他模块组装起来，用于测试业务运行。

### 本地调试
参考AlsBiz, 继承BaseClass
如果部署azkaban测试，去掉BaseClass中 .setMaster("local")代码
val config = new SparkConf().setMaster("local")


### 服务器调试代码(以shell启动)

#### 定义临时测试输出路径

/ai/data/dw/moretv_tmp/ALS

cn.moretv.doraemon.test.Constans 类中定义临时测试输出路径


#### 服务器位置

- 测试首页推荐
spark@bigdata-appsvr-130-4

/home/spark/doraemon_test/doraemon-test-1.0

参考alsModel.sh，启动cn.moretv.doraemon.test.AlsBiz类


- 测试相似影片
spark@bigdata-appsvr-130-5

/home/spark/doraemon_test/doraemon-test-1.0


#### 编译

test模块，点击package，会创建zip包上传到服务器

当改动算法、data-writer等模块，只要不引入新的jar，可以单独在模块里打包，上传到/home/spark/doraemon_test/doraemon-test-1.0/lib位置

------------------------此次上线的main入口------------------------------------

### 首页今日推荐,以后需要下线的类

hamlet

- 首页今日推荐
cn.whaley.ai.recommend.ALS.frontpage.moretv.RecommendUnionAndDistribute


测试环境：
uid,sid
/ai/data/dw/moretv_tmp/homePage/rec/20180720/11/*            214919659
/ai/data/dw/moretv_tmp/homePage/vip/20180720/11/*            108629720

生产环境：
/ai/dws/moretv/biz/homePage/vip/20180719/12/*   uid,sid      108442321
/ai/dws/moretv/biz/homePage/rec/20180719/12/*   uid,sid      189204330
/ai/dws/moretv/biz/homePage/rec/20180719/*      uid,sid      567612834
/ai/dws/moretv/biz/homePage/vip/20180719/*      uid,sid      325322833

- ALS推荐
cn.whaley.ai.recommend.ALS.frontpage.moretv.ALSRecommend

替代的类：cn.moretv.doraemon.test.AlsBiz，cn.moretv.doraemon.test.ALSRecommend

cn.moretv.doraemon.test.AlsBiz 生产环境
/ai/data/dw/moretv_tmp/ALS/item/Latest 80676
/ai/data/dw/moretv_tmp/ALS/user/Latest 26082177

测试数据位置：/ai/dw/moretv/base/als/user/Latest  10849978    
测试数据位置：/ai/dw/moretv/base/als/item/Latest  69072

cn.moretv.doraemon.test.ALSRecommend
存储格式：uid,sid,score
测试环境 /ai/tmp/ALSResult/Latest 561682600
 

- 追剧推荐
cn.whaley.ai.recommend.ALS.frontpage.moretv.SeriesChasingRecommend
替代的类：cn.moretv.doraemon.test.SeriesChasingRecommend

- 用于给用户推荐看过的最后一部电影的相似内容
cn.whaley.ai.recommend.ALS.frontpage.moretv.SimilarityRecommend 

替代的类：cn.moretv.doraemon.test.SimilarityRecommend

存储格式：uid,sid，测试数据量： 1483293，生产环境：1483479

测试数据位置：/ai/data/dw/moretv_tmp/SimilartityRecommend/Latest

生产数据位置：/ai/data/dw/moretv/SimilartityRecommend/Latest

- 长视频聚类推荐
cn.whaley.ai.recommend.ALS.frontpage.moretv.LongVideoClusterRecommend
替代的类：cn.moretv.doraemon.test.als.LongVideoClusterRecommend
存储格式：uid,sid，
测试数据位置：/ai/data/dw/moretv_tmp/longVideoClusterRecommend/Latest 1376922106
生产数据位置：/ai/data/dw/moretv/longVideoClusterRecommend/Latest     1376922106  

## TODO
地域屏蔽需要加入
azkaban打包
hdfs reader引入dataRange
spark.executor.userClassPathFirst问题
DataWriter关闭问题
首页今日推荐加入混合算子

cn.whaley.ai.recommend.ALS.frontpage.moretv.TimeDividedRecommend  `废弃`

### 相似影片







### TODO

- 临时配置问题 

spark.properties文件中,改为
spark.executor.userClassPathFirst=false
Caused by: java.io.IOException: java.lang.ClassCastException: cannot assign instance of scala.Some to field org.apache.spark.util.AccumulatorMetadata.name of type scala.Option in instance of org.apache.spark.util.AccumulatorMetadata




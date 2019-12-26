package cn.moretv.doraemon.test

import cn.moretv.doraemon.common.path.MysqlPath


object ConfigHelper {

  def getMysqlPath(tag: String): MysqlPath = {

    tag match {
      case "movie_editor_recommend" =>
        new MysqlPath("bigdata-appsvr-130-6", 3306,
          "europa", "tag_program_mapping", "bislave", "slave4bi@whaley", "id",
          Array("sid", "'movie' as cluster"),
          "tag_id = 152063 and status = 1") //筛选电影的编辑精选

      case "movie_all_sid" =>
        new MysqlPath("bigdata-appsvr-130-2", 3306,
          "mtv_cms", "mtv_basecontent", "bislave", "slave4bi@whaley", "id",
          Array("sid", "content_type as cluster"),
          "content_type = 'movie'")

      case "movie_valid_sid" => validSidPath("movie")
      case "tv_valid_sid" => validSidPath("tv")
      case "jilu_valid_sid" => validSidPath("jilu")
      case "kids_valid_sid" => validSidPath("kids")
      case "zongyi_valid_sid" => validSidPath("zongyi")
      case "comic_valid_sid" => validSidPath("comic")
      case _ => null
    }
  }

  private def validSidPath(contentType: String): MysqlPath = {
    new MysqlPath("bigdata-appsvr-130-4", 3306,
      "tvservice", "mtv_program", "bislave", "slave4bi@whaley", "id",
      Array("sid"),
      s"contentType = '$contentType' and sid is not null " +
        "and title is not null and status = 1 and type = 1")
  }
}

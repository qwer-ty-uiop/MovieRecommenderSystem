package com.ty.statistics

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.text.SimpleDateFormat
import java.util.Date


case class Movies(mId: Int, name: String, describe: String, duration: String, release: String,
                  shoot: String, language: String, genres: String, actors: String, directors: String)

case class Ratings(uId: Int, mId: Int, score: Double, timestamp: Long)

case class MongoConfig(uri: String, db: String)

// 推荐对象类
case class Recommendation(mId: Int, score: Double)

// 按类别分类的推荐列表对象
case class GenresRecommendations(genres: String, recommendations: Seq[Recommendation])


object StatisticRecommender {
  // mongo collection 名字
  private val MONGODB_MOVIE_COLLECTION = "Movie"
  private val MONGODB_RATING_COLLECTION = "Rating"
  private val MONGODB_MOST_COMMENT_MOVIE_COLLECTION = "MostCommentMovies"
  private val MONGODB_RECENT_MOST_COMMENT_MOVIE_COLLECTION = "RecentMostCommentMovies"
  private val MONGODB_MOVIE_AVERAGE_RATING_COLLECTION = "MovieAverageRating"
  private val MONGODB_GENRES_TOP_MOVIE_COLLECTION = "GenresTopMovies"

  def main(args: Array[String]): Unit = {
    val config: Map[String, String] = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )
    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
    val sparkConf = new SparkConf()
      .setMaster(config("spark.cores"))
      .setAppName("DataLoader")
    val sparkSession = new SparkSession.Builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._
    // 从 mongo 中加载数据
    val ratingDF = sparkSession.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Ratings]
      .toDF()

    val movieDF = sparkSession.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movies]
      .toDF()
    // 编写 sql 查询统计结果 (创建视图)
    ratingDF.createOrReplaceTempView("ratings")
    // TODO: 查询不同的统计推荐结果
    // 1. 历史热门统计 (历史评分最多的电影)
    val popularMoviesDF = sparkSession.sql("select mId, count(*) as count from ratings group by mId")
    saveToMongoDB(popularMoviesDF, MONGODB_MOST_COMMENT_MOVIE_COLLECTION)
    // 2. 近期热门统计 (近期评分做多的电影，按照 yyyyMM 格式的时间排序)
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    sparkSession.udf.register("changeDate", (x: Long) => simpleDateFormat.format(new Date(x * 1000))) // 注册用户自定义函数，将时间戳转为上述日期格式
    val ratingAndDate = sparkSession.sql("select mId, score, changeDate(timestamp) as date from ratings") // 对原始数据做预处理，去掉 uId
    ratingAndDate.createOrReplaceTempView("ratingAndDate")
    val recentPopularMovies = sparkSession.sql("select mId, date, count(*) as count from ratingAndDate group by date, mId order by date desc, count desc")
    saveToMongoDB(recentPopularMovies, MONGODB_RECENT_MOST_COMMENT_MOVIE_COLLECTION)
    // 3. 平均得分统计 (平均评分最高的电影)
    val movieAverageRatingDF = sparkSession.sql("select mId, avg(score) as score from ratings group by mId order by score desc")
    saveToMongoDB(movieAverageRatingDF, MONGODB_MOVIE_AVERAGE_RATING_COLLECTION)
    // 4. 各个类别电影 TOP 统计
    val movieAndScore = movieDF.join(movieAverageRatingDF, "mId")
    val genresTopMoviesDF = movieAndScore
      // 展开电影类型，避免笛卡尔积
      .flatMap { row =>
        val genres = row.getAs[String]("genres").toLowerCase.split("\\|")
        val mId = row.getAs[Int]("mId")
        val score = row.getAs[Double]("score")
        genres.map(x => (x, (mId, score)))
      }
      // 按类别分类取 TOP10
      .groupByKey(_._1)
      .mapGroups { (genres, iter) =>
        val topMovies = iter
          .map(_._2)
          .toList
          .sortBy(-_._2)
          .take(10)
          .map { case (mId, score) => Recommendation(mId, score) }
        GenresRecommendations(genres, topMovies)
      }.toDF()
    saveToMongoDB(genresTopMoviesDF, MONGODB_GENRES_TOP_MOVIE_COLLECTION)
    sparkSession.stop()
  }

  private def saveToMongoDB(df: DataFrame, collectionName: String)(implicit mongoConfig: MongoConfig): Unit = {
    df.write
      .option("uri", mongoConfig.uri)
      .option("collection", collectionName)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }
}

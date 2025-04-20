package com.ty.offline

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession

case class MovieRatings(uId: Int, mId: Int, score: Double, timestamp: Long)

case class MongoConfig(uri: String, db: String)

// 推荐对象类
case class Recommendation(mId: Int, score: Double)

// 基于预测评分的用户推荐列表对象
case class UserRecommendations(uId: Int, recommendations: Seq[Recommendation])

// 基于隐语义模型电影特征向量的电影相似度列表对象 (也是推荐列表)
case class MovieRecommendations(mId: Int, recommendations: Seq[Recommendation])

object OfflineRecommender {

  private val MONGODB_RATING_COLLECTION = "Rating"
  private val MONGODB_USER_RECOMMENDATIONS_COLLECTION = "UserRecommendations"
  private val MONGODB_MOVIE_RECOMMENDATIONS_COLLECTION = "MovieRecommendations"
  private val USER_MAX_RECOMMENDATIONS = 20

  def main(args: Array[String]): Unit = {
    // 基本配置
    val config: Map[String, String] = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender",
      "rank" -> "50",
      "iterations" -> "5",
      "regularization" -> "0.01"
    )
    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
    val sparkConf = new SparkConf()
      .setMaster(config("spark.cores"))
      .setAppName("OfflineRecommender")
    val sparkSession = new SparkSession.Builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._

    // 加载数据为RDD （spark mllib 需要用 rdd）
    val movieRatingRDD = sparkSession.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRatings]
      .rdd
      // 获取 (用户，电影，评分) 矩阵
      .map(x => Rating(x.uId, x.mId, x.score))
      // rdd 持久化再内存
      .cache()
    // 构建 user 和 movie 集合（rdd），构建 user-movie 空矩阵
    val userRDD = movieRatingRDD.map(_.user).distinct()
    val movieRDD = movieRatingRDD.map(_.product).distinct()
    val userMovieMatrix = userRDD.cartesian(movieRDD)
    /**
     * TODO: 训练隐语义模型 LFM，预测用户评分，构建用户推荐列表
     *
     * 1. 基于用户和电影的隐特征，计算预测评分，得到用户推荐列表
     * 2. 基于电影特征，计算相似度矩阵，得到电影相似度列表
     */
    val (rank, iterations, regularization) = (config("rank").toInt, config("iterations").toInt, config("regularization").toDouble)
    val model = ALS.train(movieRatingRDD, rank, iterations, regularization)
    // 模型训练完毕，调用 model 的 predict 预测用户评分
    val predRatings = model.predict(userMovieMatrix)
    val userRecommendations = predRatings
      .filter(_.rating > 0) // 过滤评分大于 0 的单元
      .map(x => (x.user, (x.product, x.rating))) // 转换为 key，value 形式，然后分组聚合
      .groupByKey()
      .map { // 转换为写入 mongodb 时设定的格式
        case (uId, recommendations) =>
          UserRecommendations(
            uId,
            recommendations
              .toList
              .sortWith(_._2 > _._2)
              .take(USER_MAX_RECOMMENDATIONS)
              .map(x => Recommendation(x._1, x._2))
          )
      }.toDF()
    // 写入 mongodb
    userRecommendations.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_USER_RECOMMENDATIONS_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    /**
     * TODO: 计算电影相似度，得到电影相似度列表
     */
    sparkSession.stop()
  }
}

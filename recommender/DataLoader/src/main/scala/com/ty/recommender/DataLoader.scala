package com.ty.recommender

import com.mongodb.client.MongoClients
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.bson.Document

/**
 * Movie 数据集
 *
 * 电影id mid
 * 电影名称 name
 * 详细描述 describe
 * 时长 duration
 * 发行时间 release
 * 拍摄时间 shoot
 * 语言 language
 * 类型 genres
 * 演员表 actors
 * 导演 directors
 */
case class Movies(mId: Int, name: String, describe: String, duration: String, release: String,
                  shoot: String, language: String, genres: String, actors: String, directors: String)

/**
 * Rating 数据集
 *
 * 用户id uid
 * 电影id mid
 * 分数 score
 * 时间戳 timestamp
 */
case class Ratings(uId: Int, mId: Int, score: Double, timestamp: Long)

/**
 * Tag 数据集
 *
 * 用户id uid
 * 电影id mid
 * 标签 tag
 * 时间戳 timestamp
 */
case class Tags(uId: Int, mId: Int, tag: String, timestamp: Long)

/**
 * 把 mongoDB 的配置封装成样例类
 *
 * @param uri mongoDB的链接
 * @param db  mongoDB数据库
 */
case class MongoConfig(uri: String, db: String)

/**
 * 把 ES 的配置封装成样例类
 *
 * @param httpHosts      http 主机列表
 * @param transportHosts transport 主机列表
 * @param index          需要操作的索引
 * @param clusterName    集群名称，默认elasticsearch
 */
case class ESConfig(httpHosts: String, transportHosts: String, index: String, clusterName: String)

object DataLoader {
  private val MOVIE_DATA_PATH = "recommender/DataLoader/src/main/resources/movies.csv"
  private val RATING_DATA_PATH = "recommender/DataLoader/src/main/resources/ratings.csv"
  private val TAG_DATA_PATH = "recommender/DataLoader/src/main/resources/tags.csv"
  private val MONGODB_MOVIE_COLLECTION = "Movie"
  private val MONGODB_RATING_COLLECTION = "Rating"
  private val MONGODB_TAG_COLLECTION = "Tag"
  private val ES_MOVIE_INDEX = "_doc"

  def main(args: Array[String]): Unit = {
    val config: Map[String, String] = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender",
      "es.httpHosts" -> "localhost:9200",
      "es.transportHosts" -> "localhost:9300",
      "es.index" -> "recommender",
      "es.cluster.name" -> "elasticsearch"
    )
    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
    implicit val esConfig: ESConfig = ESConfig(config("es.httpHosts"), config("es.transportHosts"), config("es.index"), config("es.cluster.name"))
    // 创建 sparkConf
    val sparkConf = new SparkConf()
      .setMaster(config("spark.cores"))
      .set("es.net.http.auth.user", "elastic")
      .set("es.net.http.auth.pass", "elastic")
      .setAppName("DataLoader")
    // 创建 sparkSession
    val sparkSession = new SparkSession.Builder().config(sparkConf).getOrCreate()
    // 引入 sparkSession 隐式包
    import sparkSession.implicits._

    // 加载数据、数据预处理
    val movieRDD = sparkSession.sparkContext.textFile(MOVIE_DATA_PATH)
    val movieDF = movieRDD.map(
      item => {
        val parts = item.split("\\^")
        Movies(parts(0).toInt, parts(1).trim, parts(2).trim, parts(3).trim, parts(4).trim, parts(5).trim, parts(6).trim, parts(7).trim, parts(8).trim, parts(9).trim)
      }
    ).toDF()

    val ratingRDD = sparkSession.sparkContext.textFile(RATING_DATA_PATH)
    val ratingDF = ratingRDD.map(
      item => {
        val parts = item.split(",")
        Ratings(parts(0).toInt, parts(1).toInt, parts(2).toDouble, parts(3).toLong)
      }
    ).toDF()

    val tagRDD = sparkSession.sparkContext.textFile(TAG_DATA_PATH)
    val tagDF = tagRDD.map(
      item => {
        val parts = item.split(",")
        Tags(parts(0).toInt, parts(1).toInt, parts(2).trim, parts(3).toLong)
      }
    ).toDF()
    // 保存到 mongoDB
    saveToMongoDB(movieDF, ratingDF, tagDF)

    // 预处理，把 tag 集成到 movie 数据中
    import org.apache.spark.sql.functions._

    val newTag = tagDF.groupBy($"mId")
      .agg(concat_ws("|", collect_set($"tag")).as("tags"))
      .select("mId", "tags")

    // 对 newTag 和 movie 做 join 操作
    val movieWithTagDF = movieDF.join(newTag, Seq("mId"), "left")

    // 保存到 elasticsearch
    saveToES(movieWithTagDF)
    // 关闭 sparkSession
    sparkSession.stop()
  }

  private def saveToMongoDB(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit = {
    // 新建一个 mongodb 的连接
    val mongoClient = MongoClients.create(mongoConfig.uri)
    val database = mongoClient.getDatabase(mongoConfig.db)
    // 如果 mongodb 已经有了相应的数据库，需要先删除
    database.getCollection(MONGODB_MOVIE_COLLECTION).drop()
    database.getCollection(MONGODB_RATING_COLLECTION).drop()
    database.getCollection(MONGODB_TAG_COLLECTION).drop()
    // 将 df 写入对应的 mongodb 表中
    movieDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    tagDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_TAG_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    // 建立索引
    database.getCollection(MONGODB_MOVIE_COLLECTION).createIndex(new Document("mId", 1))
    database.getCollection(MONGODB_RATING_COLLECTION).createIndex(new Document("uId", 1))
    database.getCollection(MONGODB_RATING_COLLECTION).createIndex(new Document("mId", 1))
    database.getCollection(MONGODB_TAG_COLLECTION).createIndex(new Document("uId", 1))
    database.getCollection(MONGODB_TAG_COLLECTION).createIndex(new Document("mId", 1))
    mongoClient.close()
  }

  private def saveToES(movieDF: DataFrame)(implicit esConfig: ESConfig): Unit = {
    movieDF.write
      .option("es.nodes", esConfig.httpHosts)
      .option("es.http.timeout", "100m")
      .option("es.mapping.id", "mId")
      .option("es.index.auto.create", "true")
      .mode("overwrite")
      .format("org.elasticsearch.spark.sql")
      .save(esConfig.index + "/" + ES_MOVIE_INDEX)
  }

}

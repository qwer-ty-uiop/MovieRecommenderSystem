package com.ty.recommender

import com.mongodb.casbah.Imports.{MongoClient, MongoClientURI}
import com.mongodb.casbah.commons.MongoDBObject
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

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

class DataLoader {
  private val MOVIE_DATA_PATH = "src/main/resources/movies.csv"
  private val RATING_DATA_PATH = "src/main/resources/ratings.csv"
  private val TAG_DATA_PATH = "src/main/resources/tags.csv"
  private val MONGODB_MOVIE_COLLECTION = "Movie"
  private val MONGODB_RATING_COLLECTION = "Rating"
  private val MONGODB_TAG_COLLECTION = "Tag"
  val ES_MOVIE_INDEX = "Movie"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender",
      "es.httpHosts" -> "localhost:9200",
      "es.transportHosts" -> "localhost:9300",
      "es.index" -> "recommender",
      "es.cluster.name" -> "elasticsearch"
    )
    // 创建 sparkConf
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")
    // 创建 sparkSession
    val sparkSession = new SparkSession.Builder().config(sparkConf).getOrCreate()
    // 引入 sparkSession 隐式包
    import sparkSession.implicits._

    // 加载数据、数据预处理
    val movieRDD = sparkSession.sparkContext.textFile(MOVIE_DATA_PATH)
    val movieDF = movieRDD.map(
      item => {
        val parts = item.split("\\^")
        Movies(parts(0).toInt, parts(1).strip(), parts(2).strip(), parts(3).strip(), parts(4).strip(), parts(5).strip(), parts(6).strip(), parts(7).strip(), parts(8).strip(), parts(9).strip())
      }
    ).toDF()

    val ratingRDD = sparkSession.sparkContext.textFile(RATING_DATA_PATH)
    val ratingDF = movieRDD.map(
      item => {
        val parts = item.split(",")
        Ratings(parts(0).toInt, parts(1).toInt, parts(2).toDouble, parts(3).toLong)
      }
    ).toDF()

    val tagRDD = sparkSession.sparkContext.textFile(TAG_DATA_PATH)
    val tagDF = movieRDD.map(
      item => {
        val parts = item.split("\\^")
        Tags(parts(0).toInt, parts(1).toInt, parts(2).strip(), parts(3).toLong)
      }
    ).toDF()
    // 保存到 mongoDB
    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
    saveToMongoDB(movieDF, ratingDF, tagDF)
    // 保存到 elasticsearch
    saveToES()
    // 关闭 sparkSession
    sparkSession.stop()
  }

  private def saveToMongoDB(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit = {
    // 新建一个 mongodb 的连接
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    // 如果 mongodb 已经有了相应的数据库，需要先删除
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).dropCollection()
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
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mId" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("uId" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("mId" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("uId" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("mId" -> 1))
    mongoClient.close()
  }

  private def saveToES(): Unit = {

  }

}

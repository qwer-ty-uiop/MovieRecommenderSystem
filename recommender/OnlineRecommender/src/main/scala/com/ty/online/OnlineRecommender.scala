package com.ty.online

import com.mongodb.client.{MongoClient, MongoClients}
import com.mongodb.client.model.Filters
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.bson.Document
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.math.log

// 推荐对象类
case class Recommendation(mId: Int, score: Double)

// 电影相似度列表对象 (也是推荐列表)
case class MovieRecommendations(mId: Int, recommendations: Seq[Recommendation])

// mongodb 连接助手
object ConnectHelper extends Serializable {
  lazy val jedis = new Jedis("192.168.207.202")
  lazy val mongoClient: MongoClient = MongoClients.create("mongodb://localhost:27017/recommender")
}

object OnlineRecommender {

  private val MONGODB_RATING_COLLECTION = "Rating"
  private val MONGODB_ONLINE_RECOMMENDATIONS_COLLECTION = "OnlineRecommendations"
  private val MONGODB_MOVIE_RECOMMENDATIONS_COLLECTION = "MovieRecommendations"
  private val USER_MAX_COMMENT_NUM = 20
  private val MOVIE_MAX_SIMILARITY_NUM = 20

  def main(args: Array[String]): Unit = {
    // 初始化配置信息
    implicit val config: Map[String, String] = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender",
      "kafka.topic" -> "recommender"
    )
    val sparkConf = new SparkConf()
      .setMaster(config("spark.cores"))
      .setAppName("OnlineRecommender")
    val sparkSession = new SparkSession.Builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._
    val sparkStreamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(2)) // batch duration，设置批处理时间限制

    // 获取数据，加载数据后广播，一个 executor 上留存一个副本，节省内存，提高性能
    val movieSimilarityRDD = sparkSession.read
      .option("uri", config("mongo.uri"))
      .option("collection", MONGODB_MOVIE_RECOMMENDATIONS_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRecommendations]
      .rdd // 转为 rdd 有更多方法可以用，然后将 recommendations: Seq 对象转为元组，再调用 toMap，变为字典对象，再将数据中 key 相同的对象收集起来，作为 Map 对象
      .map(x => (x.mId, x.recommendations.map(recommendation => (recommendation.mId, recommendation.score)).toMap))
      .collectAsMap()
    // 将数据广播（传入数据到流中）
    val movieSimilarityBroadcast = sparkSession.sparkContext.broadcast(movieSimilarityRDD)
    // 定义 kafka 连接参数
    val kafkaParam = Map(
      "bootstrap.servers" -> "192.168.207.202:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender",
      "auto.offset.reset" -> "latest"
    )
    // 通过 kafka 创建 DStream （输入数据流）
    val kafkaDStream = KafkaUtils.createDirectStream[String, String](
      ssc = sparkStreamingContext,
      locationStrategy = LocationStrategies.PreferConsistent,
      consumerStrategy = ConsumerStrategies.Subscribe[String, String](Array(config("kafka.topic")), kafkaParam)
    )
    // 定义处理流的操作，把原始数据 uid|mid|score|timestamp 转化为评分流（将 Kafka 消息流 kafkaDStream 转换为结构化的评分数据流 ratingStream）
    val ratingStream = kafkaDStream.map {
      msg =>
        val parts = msg.value().split("\\|")
        (parts(0).toInt, parts(1).toInt, parts(2).toDouble, parts(3).toLong)
    }

    /**
     * TODO：实现实时算法
     *
     * 1. 从 redis 中获取当前用户的 k 次评分 => Array[(mid, score)]
     * 2. 从 相似度矩阵中，取出当前电影最相似的 n 个电影作为备选列表 (初步筛选推荐电影) => Array[mid]
     * 3. 根据用户最近评分和电影相似度向量，计算每个电影的优先级 (公式二次筛选)，得到当前用户的试试推荐列表 => Array[(mid, score)]
     * 4. 把推荐数据保存到 mongodb 中
     */
    ratingStream.foreachRDD {
      rdd =>
        rdd.foreach {
          case (uId, mId, _, _) =>
            println("-----------------------------Rating Data Coming Soon-----------------------------")
            // 获取用户最近的评分
            val userRecentComment = getUserRecentComment(uId, USER_MAX_COMMENT_NUM, ConnectHelper.jedis)

            println(userRecentComment.mkString("user recentComment (mId, score): ", ", ", ""))

            // 相似矩阵已经放入广播变量中了；uId 用于过滤已经看过的电影；需要进入 mongodb 中查看已经评分过的电影
            val alternativeMovies = getAlternativeMovies(uId, mId, movieSimilarityBroadcast.value, MOVIE_MAX_SIMILARITY_NUM)

            println(alternativeMovies.mkString("alternative movies (mId): ", ", ", ""))

            // 获取推荐列表
            val curRecommendations = calculateMoviePriority(userRecentComment, alternativeMovies, movieSimilarityBroadcast.value)

            println(curRecommendations.mkString("recommended movies (mId, score): ", ", ", ""))

            // 保存到 mongodb
            saveToMongodb(uId, curRecommendations)
        }
    }
    // 启动流处理上下文，开始接收和处理数据
    sparkStreamingContext.start()
    println("Streaming Started")
    sparkStreamingContext.awaitTermination()
  }

  private def saveToMongodb(uId: Int, curRecommendations: Array[(Int, Double)])(implicit config: Map[String, String]): Unit = {
    val onlineRecommendationsCollection = ConnectHelper.mongoClient
      .getDatabase(config("mongo.db"))
      .getCollection(MONGODB_ONLINE_RECOMMENDATIONS_COLLECTION)
    // 1. 如果表中已经有了 uId 对应的数据，先删除
    onlineRecommendationsCollection.findOneAndDelete(Filters.eq("uId", uId))

    // 2. 构建需要插入的新文档
    val doc = new Document()
      .append("uId", uId)
      .append("recommendations", curRecommendations.map { case (mid, score) =>
        new Document()
          .append("movieId", mid)
          .append("score", score)
      }.toList.asJava)

    // 3. 执行插入操作
    onlineRecommendationsCollection.insertOne(doc)
  }

  /**
   * 计算候选推荐电影的推荐权重
   *
   * @param userRecentComment 用户近期评分过的电影
   * @param alternativeMovies 侯选电影列表
   * @param movieSimilarity   电影相似度矩阵
   * @return 候选推荐电影的推荐权重列表
   */
  private def calculateMoviePriority(userRecentComment: Array[(Int, Double)], alternativeMovies: Array[Int], movieSimilarity: collection.Map[Int, Map[Int, Double]]): Array[(Int, Double)] = {
    // 定义 ArrayBuffer，用于保存每个电影的基础得分
    val scores = scala.collection.mutable.ArrayBuffer[(Int, Double)]()
    // 保存每个可推荐电影的 "增强、减弱因子" => (lg_max - lg_min); 增强因子就是与用户喜好电影相似度高的电影数量，减弱因子则相反
    val enhanceMap = scala.collection.mutable.HashMap[Int, Int]()
    val weakenMap = scala.collection.mutable.HashMap[Int, Int]()

    for (alterMovie <- alternativeMovies; userComment <- userRecentComment) {
      // 计算相似度得分
      val similarScore = getMovieSimilarScore(alterMovie, userComment._1, movieSimilarity)
      // 计算可选电影的基础推荐得分 (相似度大于 0.7 才考虑下一步处理)
      if (similarScore > 0.7) {
        scores += ((alterMovie, similarScore * userComment._2))
        if (userComment._2 > 3) {
          enhanceMap(alterMovie) = enhanceMap.getOrDefault(alterMovie, 1) + 1
        } else {
          weakenMap(alterMovie) = weakenMap.getOrDefault(alterMovie, 1) + 1
        }
      }
    }
    // 得到基础得分、增强、减弱因子，根据公式计算每个可选电影的最终推荐权重，并返回
    scores.groupBy(_._1)
      .map {
        case (mId, scoreList) =>
          (
            mId,
            scoreList.map(_._2).sum / scoreList.length
              + log(enhanceMap.getOrDefault(mId, 1))
              - log(weakenMap.getOrDefault(mId, 1))
          )
      }
      .toArray
      .sortWith(_._2 > _._2)
  }

  private def getMovieSimilarScore(mid1: Int, mid2: Int, movieSimilarity: collection.Map[Int, Map[Int, Double]]): Double = {
    movieSimilarity.getOrElse(mid1, Map.empty).getOrElse(mid2, 0.0)
  }


  /**
   * 获取推荐电影列表
   *
   * @param uId                      用户 id
   * @param mId                      参考的电影 id
   * @param movieSimilarity          电影相似度矩阵
   * @param MOVIE_MAX_SIMILARITY_NUM 相似度推荐列表的最大长度
   * @param config                   mongodb 连接的配置项
   * @return 相似度推荐列表
   */
  private def getAlternativeMovies(uId: Int, mId: Int, movieSimilarity: collection.Map[Int, Map[Int, Double]], MOVIE_MAX_SIMILARITY_NUM: Int)(implicit config: Map[String, String]): Array[Int] = {
    // 1. 从相似度矩阵中获取所有相似电影
    val curSimilarMovies = movieSimilarity(mId).toArray
    // 2. 从 mongodb 中查询用户已经看过的电影
    val seenBefore = ConnectHelper.mongoClient
      .getDatabase(config("mongo.db"))
      .getCollection(MONGODB_RATING_COLLECTION)
      .find(Filters.eq("uId", uId))
      .toArray
      .map(x => x.get("mId").toString.toInt)
    // 3. 过滤已经看过的电影
    curSimilarMovies.filter(x => !seenBefore.contains(x._1))
      .sortWith(_._2 > _._2)
      .take(MOVIE_MAX_SIMILARITY_NUM)
      .map(_._1)
  }

  /**
   * 获取用户最近评分数据
   *
   * @param uId                  用户 id
   * @param USER_MAX_COMMENT_NUM 用户最近评分列表的最大长度
   * @param jedis                redis 连接
   * @return 用户最近评分数据列表
   */
  private def getUserRecentComment(uId: Int, USER_MAX_COMMENT_NUM: Int, jedis: Jedis): Array[(Int, Double)] = {
    // 从 redis 读取用户评分，redis里的数据保存在以 UID 为 key 的队列中，队列里面的 values 是一组评分 MID:SCORE
    jedis.lrange("uId:" + uId, 0, USER_MAX_COMMENT_NUM - 1)
      .map { // 此处需要引入 java 转 scala 的库，因为 jedis 是 java 与 redis 的 api 接口
        x =>
          val parts = x.split(":")
          (parts(0).trim.toInt, parts(1).trim.toDouble)
      }
      .toArray
  }

}

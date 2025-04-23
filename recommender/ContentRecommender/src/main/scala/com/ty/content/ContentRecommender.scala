package com.ty.content

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix

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

// 推荐对象类
case class Recommendation(mId: Int, score: Double)

// 基于电影内容信息提取出的特征向量，计算得出的电影相似度列表对象 (也是推荐列表)
case class MovieRecommendations(mId: Int, recommendations: Seq[Recommendation])

object ContentRecommender {

  private val MONGODB_MOVIE_COLLECTION = "Movie"
  private val MONGODB_MOVIE_RECOMMENDATIONS_COLLECTION = "ContentBasedMovieRecommendations"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("ContentRecommender")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._

    val movies = sparkSession.read
      .option("uri", config("mongo.uri"))
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movies]
      .map(x => (x.mId, x.name, x.describe, x.language, x.directors, x.genres.split("\\|")))
      .toDF("mId", "name", "describe", "language", "directors", "genres")
      .cache

    /**
     * TODO: 用 TF-IDF 从内容信息中提取电影特征向量
     *
     * 1. 统计 TF（词频）引入 hashing TF，把一个词语序列转化为对应的词频
     * 2. 统计 IDF（逆文档频率）引入 idf 工具
     * 3. 将处理完的数据转为 "特征向量"
     */
    val hashingTF = new HashingTF().setInputCol("genres").setOutputCol("termFrequency").setNumFeatures(50)
    val moviesTF = hashingTF.transform(movies)
    val idf = new IDF().setInputCol("termFrequency").setOutputCol("TF-IDF")
    // 训练 idf 模型，得到每个词的逆文档频率
    val idfModel = idf.fit(moviesTF)
    // 用模型对原数据处理，得到每个词的 tf-idf 值，作为新的特征向量
    val moviesTFIDF = idfModel.transform(moviesTF)
    // 根据类别信息计算电影相似度
    val movieFeatures = moviesTFIDF
      .map(row => (row.getAs[Int]("mId"), row.getAs[SparseVector]("TF-IDF").toArray))
      .rdd
      .map(x => (x._1, new DoubleMatrix(x._2)))
    // 做笛卡尔积，过滤自己和自己的笛卡尔积，然后计算两两电影的余弦相似度
    val movieRecommendations = movieFeatures.cartesian(movieFeatures)
      .filter { case (movieA, movieB) => movieA._1 != movieB._1 } // 将 mId 相同的电影过滤掉
      .map {
        case (movieA, movieB) =>
          val similarScore = this.cosSimilarity(movieA._2, movieB._2)
          (movieA._1, (movieB._1, similarScore))
      }
      .filter(_._2._2 > 0.6) // 过滤出相似度大于 0.6 的电影
      .groupByKey()
      .map {
        case (mId, recommendations) =>
          MovieRecommendations(mId,
            recommendations
              .toList
              .sortWith(_._2 > _._2)
              .map(x => Recommendation(x._1, x._2)))
      }.toDF()

    // 写入 mongodb
    movieRecommendations.write
      .option("uri", config("mongo.uri"))
      .option("collection", MONGODB_MOVIE_RECOMMENDATIONS_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    sparkSession.stop()
  }

  /**
   * 求向量的余弦相似度
   *
   * @param movieA 电影 A 特征向量
   * @param movieB 电影 B 特征向量
   */
  private def cosSimilarity(movieA: DoubleMatrix, movieB: DoubleMatrix): Double = {
    movieA.dot(movieB) / (movieA.norm2() * movieB.norm2())
  }
}

package com.ty.offline

import breeze.numerics.sqrt
import com.ty.offline.OfflineRecommender.MONGODB_RATING_COLLECTION
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ALSTrainer {
  def main(args: Array[String]): Unit = {
    val config: Map[String, String] = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )
    val sparkConf = new SparkConf()
      .setMaster(config("spark.cores"))
      .setAppName("ALSTrainer")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 注册所有可能被序列化的类（包括 ALS 模型内部类）
      .registerKryoClasses(
        Array(
          classOf[Rating],
          classOf[MatrixFactorizationModel],
          // 添加其他自定义类（如 MovieRatings, MongoConfig）
          classOf[com.ty.offline.MovieRatings],
          classOf[com.ty.offline.MongoConfig]
        )
      )
    val sparkSession = new SparkSession.Builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._

    // 加载评分数据
    val movieRatingRDD = sparkSession.read
      .option("uri", config("mongo.uri"))
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRatings]
      .rdd
      // 获取 (用户，电影，评分) 矩阵
      .map(x => Rating(x.uId, x.mId, x.score))
      // rdd 持久化再内存
      .cache()
    // 切分数据集和测试集
    val Array(trainRDD, testRDD) = movieRatingRDD.randomSplit(Array(0.8, 0.2))
    // 选择模型参数，并输出最优参数
    tuneModelParameters(trainRDD, testRDD)
    sparkSession.close()
  }

  private def tuneModelParameters(trainRDD: RDD[Rating], testRDD: RDD[Rating]): Unit = {
    val result = for (rank <- Array(10, 20, 50); regularization <- Array(0.0001, 0.001, 0.1))
      yield {
        val model = ALS.train(trainRDD, rank, 5, regularization)
        val RMSE = getRMSE(model, testRDD)
        (rank, regularization, RMSE)
      }
    println(result.minBy(_._3))
  }

  private def getRMSE(model: MatrixFactorizationModel, data: RDD[Rating]): Double = {
    // 用模型预测测试集数据
    val userMovie = data.map { x => (x.user, x.product) }
    val predictRatings = model.predict(userMovie)
    // 联表查询
    val y = data.map(x => ((x.user, x.product), x.rating))
    val pred_y = predictRatings.map(x => ((x.user, x.product), x.rating))
    sqrt(
      y.join(pred_y)
      .map { case ((_, _), (actual, predict)) => (predict - actual) * (predict - actual) }
      .mean()
    )
  }


}

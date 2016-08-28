package Recommender

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Recommender extends App {
    def removeDuplicateRatings(ratingPair: (Types.User, (Types.Rating, Types.Rating))): Boolean = {
        val (_, ((item1, _), (item2, _))) = ratingPair
        item1 < item2
    }

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConfig = new SparkConf()
        .setMaster("local[*]")
        .setAppName("Recommender")

    implicit val sc = new SparkContext(sparkConfig)

    val itemsSource = File("./data/ml-100k/u.item", "|")
    val items: RDD[Types.Item] = DataSource.items(itemsSource, 0, 1)

    val ratingsSource = File("./data/ml-100k/u.data", "\t")
    val ratings: RDD[Types.UserRating] = DataSource.ratings(ratingsSource, 0, 1, 2)
    val ratingsPairs = ratings.join(ratings).filter(removeDuplicateRatings)

    ratingsPairs.take(10).foreach(println)

    sc.stop()
}

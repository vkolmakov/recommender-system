package Recommender

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Recommender extends App {
    def removeDuplicateRatings(userRatingsPair: (Serialized.User, (Serialized.Rating, Serialized.Rating))): Boolean = {
        val (_, ((item1, _), (item2, _))) = userRatingsPair
        item1 < item2
    }

    def groupRatingPairs(userRatingsPair: (Serialized.User, (Serialized.Rating, Serialized.Rating))) = {
        val (_, ((item1, rating1), (item2, rating2))) = userRatingsPair
        ((item1, item2), (rating1, rating2))
    }

    def cosineSimilarity(ratingPairs: Iterable[(Double, Double)]): Serialized.Similarity = {
        val xx = ratingPairs.map({ case (x, _) => Math.pow(x, 2) }).reduce(_ + _)
        val yy = ratingPairs.map({ case (_, y) => Math.pow(y, 2) }).reduce(_ + _)
        val xy = ratingPairs.map({ case (x, y) => x * y }).reduce(_ + _)
        val count = ratingPairs.size

        xy match {
            case xy if xy != 0 => (Math.sqrt(xx) * Math.sqrt(yy) / xy, count)
            case _ => (0, count)
        }
    }

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConfig = new SparkConf()
        .setMaster("local[*]")
        .setAppName("Recommender")

    implicit val sc = new SparkContext(sparkConfig)

    val itemsSource = File("./data/ml-100k/u.item", "|")
    val items = DataSource.items(itemsSource, 0, 1)

    val userRatingsSource = File("./data/ml-100k/u.data", "\t")
    val userRatings: RDD[Serialized.UserRating] = DataSource.userRatings(userRatingsSource, 0, 1, 2)
    val userRatingPairs = userRatings.join(userRatings).filter(removeDuplicateRatings)
    val itemRatingPairs = userRatingPairs.map(groupRatingPairs).groupByKey()
    val itemPairSimilarities = itemRatingPairs.mapValues(cosineSimilarity)

    itemPairSimilarities.take(10).foreach(println)

    sc.stop()
}

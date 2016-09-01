package Recommender

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}


object Recommender extends App {
    def cosineSimilarity(ratingPairs: Iterable[(Double, Double)]): Option[Similarity] = {
        val xx = ratingPairs.map({ case (x, _) => Math.pow(x, 2) }).sum
        val yy = ratingPairs.map({ case (_, y) => Math.pow(y, 2) }).sum
        val xy = ratingPairs.map({ case (x, y) => x * y }).sum
        val count = ratingPairs.size

        xy match {
            case xy if xy != 0 => Some(Similarity(xy / Math.sqrt(xx) * Math.sqrt(yy), count))
            case _ => None
        }
    }

    def getItemRatingPairs(userRatings: Dataset[UserRating]) = {
        import spark.implicits._

        val ratings = userRatings.map(ur => (ur.user.id, (ur.rating.itemId, ur.rating.value))).rdd
        val ratingPairs = ratings.join(ratings)

        ratingPairs.filter({ case (_, ((id1, _), (id2, _))) => id1 > id2})
            .map({ case (_, ((id1, value1), (id2, value2))) => ((id1, id2), (value1, value2))})

    }

    Logger.getLogger("org").setLevel(Level.ERROR)
    implicit val spark = SparkSession
        .builder()
        .appName("Recommender")
        .master("local[*]")
        .getOrCreate()

    val itemsSource = File("./data/ml-100k/u.item", "|")
    val items = DataSource.items(itemsSource, 0, 1)

    val userRatingsSource = File("./data/ml-100k/u.data", "\t")
    val userRatings: Dataset[UserRating] = DataSource.userRatings(userRatingsSource, 0, 1, 2)

    val itemRatingPairs = getItemRatingPairs(userRatings)
    val itemSimilarities = itemRatingPairs.groupByKey().flatMapValues(cosineSimilarity)

    spark.stop()
}

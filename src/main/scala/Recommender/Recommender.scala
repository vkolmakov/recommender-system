package Recommender

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}


object Recommender extends App {
    def cosineSimilarity(ratingPairs: Iterable[(Double, Double)]): Option[Similarity] = {
        val xx = ratingPairs.map({ case (x, _) => Math.pow(x, 2) }).sum
        val yy = ratingPairs.map({ case (_, y) => Math.pow(y, 2) }).sum
        val xy = ratingPairs.map({ case (x, y) => x * y }).sum
        val count = ratingPairs.size

        xy match {
            case xy if xy != 0 => Some(Similarity(xy / (Math.sqrt(xx) * Math.sqrt(yy)), count))
            case _ => None
        }
    }

    def getItemRatingPairs(userRatings: Dataset[(User, Rating)]): RDD[((Int, Int), (Double, Double))] = {
        import spark.implicits._

        val ratings = userRatings.map({ case (user, rating) => (user.id, (rating.itemId, rating.value)) }).rdd
        val ratingPairs = ratings.join(ratings)
            .filter { case (_, ((id1, _), (id2, _))) => id1 < id2 }

        ratingPairs.map { case (_, ((id1, value1), (id2, value2))) => ((id1, id2), (value1, value2)) }
    }

    def isRelatedSimilarity(targetItemId: Int, minSimilarityStrength: Int = 50, minSimilarityValue: Double = 0.90)(itemToItemSimilarity: ((Int, Int), Similarity)) = {
        val ((id1, id2), similarity) = itemToItemSimilarity
        val isRelated = id1 == targetItemId || id2 == targetItemId
        val isStrongEnough = similarity.strength > minSimilarityStrength && similarity.value > minSimilarityValue

        isRelated && isStrongEnough
    }

    def keepOtherSimilarity(targetItemId: Int)(itemToItemSimilarity: ((Int, Int), Similarity)) = {
        val ((id1, id2), similarity) = itemToItemSimilarity

        (if (id1 == targetItemId) id2 else id1, similarity)
    }

    Logger.getLogger("org").setLevel(Level.ERROR)

    implicit val spark = SparkSession
        .builder()
        .appName("Recommender")
        .master("local[*]")
        .getOrCreate()

    import spark.implicits._

    val itemsSource = File("./data/ml-100k/u.item", '|')
    val items = DataSource.items(itemsSource, 0, 1)

    val userRatingsSource = File("./data/ml-100k/u.data", '\t')
    val userRatings: Dataset[(User, Rating)] = DataSource.userRatings(userRatingsSource, 0, 1, 2)

    val itemRatingPairs = getItemRatingPairs(userRatings)
    val itemToItemSimilarities: RDD[((Int, Int), Similarity)] = itemRatingPairs.groupByKey().flatMapValues(cosineSimilarity)

    val TARGET_ITEM_ID = 50

    val relatedItems = itemToItemSimilarities.filter(isRelatedSimilarity(TARGET_ITEM_ID))
        .map(keepOtherSimilarity(TARGET_ITEM_ID))
        .toDS()

    val results = relatedItems.as("relatedItems").joinWith(items.as("items"), $"relatedItems._1" === $"items.id")
        .map({ case ((_, similarity), item) => (item, similarity) })
        .sort($"_2.value".desc)

    results.take(10).foreach(println)

    spark.stop()
}

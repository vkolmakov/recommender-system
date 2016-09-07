package Recommender

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

case class Recommender(itemSource: DataSource, userRatingSource: DataSource)(implicit spark: SparkSession) {
    private val items: Dataset[Item] = DataSource.items(itemSource, 0, 1)
    private val userRatings: Dataset[(User, Rating)] = DataSource.userRatings(userRatingSource, 0, 1, 2)

    private val itemToItemSimilarities: RDD[((Int, Int), Similarity)] = build(itemSource, userRatingSource)

    def recommendationsFor(itemId: Int) = {
        import spark.implicits._
        val TARGET_ITEM_ID = itemId
        val relatedItems = itemToItemSimilarities
            .filter(isRelatedSimilarity(TARGET_ITEM_ID))
            .map(keepOtherSimilarity(TARGET_ITEM_ID))
            .toDS()

        val results = relatedItems.as("relatedItems")
            .joinWith(items.as("items"), $"relatedItems._1" === $"items.id")
            .map({ case ((_, similarity), item) => (item, similarity) })
            .sort($"_2.value".desc)

        results.take(10)
    }

    private def build(itemSource: DataSource, userRatingSource: DataSource) = {
        val itemRatingPairs = getItemRatingPairs(userRatings)
        val itemToItemSimilarities: RDD[((Int, Int), Similarity)] = itemRatingPairs.groupByKey().flatMapValues(cosineSimilarity)

        itemToItemSimilarities
    }

    private def cosineSimilarity(ratingPairs: Iterable[(Double, Double)]): Option[Similarity] = {
        val xx = ratingPairs.map({ case (x, _) => Math.pow(x, 2) }).sum
        val yy = ratingPairs.map({ case (_, y) => Math.pow(y, 2) }).sum
        val xy = ratingPairs.map({ case (x, y) => x * y }).sum
        val count = ratingPairs.size

        xy match {
            case xy if xy != 0 => Some(Similarity(xy / (Math.sqrt(xx) * Math.sqrt(yy)), count))
            case _ => None
        }
    }

    private def getItemRatingPairs(userRatings: Dataset[(User, Rating)]): RDD[((Int, Int), (Double, Double))] = {
        import spark.implicits._

        val ratings = userRatings.map({ case (user, rating) => (user.id, (rating.itemId, rating.value)) }).rdd
        val ratingPairs = ratings.join(ratings)
            .filter { case (_, ((id1, _), (id2, _))) => id1 < id2 }

        ratingPairs.map { case (_, ((id1, value1), (id2, value2))) => ((id1, id2), (value1, value2)) }
    }

    private def isRelatedSimilarity(targetItemId: Int, minSimilarityStrength: Int = 50, minSimilarityValue: Double = 0.90)(itemToItemSimilarity: ((Int, Int), Similarity)) = {
        val ((id1, id2), similarity) = itemToItemSimilarity
        val isRelated = id1 == targetItemId || id2 == targetItemId
        val isStrongEnough = similarity.strength > minSimilarityStrength && similarity.value > minSimilarityValue

        isRelated && isStrongEnough
    }

    private def keepOtherSimilarity(targetItemId: Int)(itemToItemSimilarity: ((Int, Int), Similarity)) = {
        val ((id1, id2), similarity) = itemToItemSimilarity

        (if (id1 == targetItemId) id2 else id1, similarity)
    }
}

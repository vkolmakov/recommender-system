package Recommender

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

object Recommender {
    def apply(itemSource: DataSource, userRatingSource: DataSource)(implicit spark: SparkSession) = Recommender(itemSource, userRatingSource)

    case class Recommender(itemSource: DataSource, userRatingSource: DataSource)(implicit spark: SparkSession) {

        import spark.implicits._

        private val items: Dataset[Item] = DataSource.items(itemSource, 0, 1)
        private val userRatings: Dataset[(User, Rating)] = DataSource.userRatings(userRatingSource, 0, 1, 2)

        private val itemToItemSimilarities: Dataset[((Int, Int), Similarity)] = build(itemSource, userRatingSource).cache()

        private val minSimilarityStrength: Int = 50
        private val minSimilarityValue: Double = 0.90
        private val minRatingValue: Double = 3.0

        def recommendationsForItem(item: Item, recommendationsCount: Int = 10): Array[(Item, Similarity)] = {
            val relatedItems = itemToItemSimilarities
                .filter(isRelatedSimilarity(item.id)(_))
                .map(keepOtherSimilarity(item.id)(_))

            val results = relatedItems.as("relatedItems")
                .joinWith(items.as("items"), $"relatedItems._1" === $"items.id")
                .map({ case ((_, similarity), item) => (item, similarity) })
                .sort($"_2.value".desc)

            results.take(recommendationsCount)
        }

        def recommendationsForUser(user: User, recommendationsCount: Int = 10): Array[(Item, Similarity)] = {
            val ratings = userRatings
                .filter(ur => ur._1.id == user.id && ur._2.value > minRatingValue)
                .map({ case (_, rating) => rating })

            val itemIds = ratings.as("rr")
                .joinWith(items.as("items"), $"rr.itemId" === $"items.id")
                .map({ case (rating, item) => item.id })
                .collect()

            val relatedItems = itemToItemSimilarities
                .filter(isRelatedSimilarity(itemIds)(_))
                .map(keepOtherSimilarity(itemIds)(_))
                .dropDuplicates(Array("_1")) // remove duplicates by itemId

            val results = relatedItems.as("relatedItems")
                .joinWith(items.as("items"), $"relatedItems._1" === $"items.id")
                .map({ case ((_, similarity), item) => (item, similarity) })
                .sort($"_2.value".desc)

            results.take(recommendationsCount)
        }

        private def build(itemSource: DataSource, userRatingSource: DataSource) = {
            val itemRatingPairs = getItemRatingPairs(userRatings)
            val itemToItemSimilarities: Dataset[((Int, Int), Similarity)] = itemRatingPairs.groupByKey().flatMapValues(cosineSimilarity).toDS

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
            val ratings = userRatings.map({ case (user, rating) => (user.id, (rating.itemId, rating.value)) }).rdd
            val ratingPairs = ratings.join(ratings)
                .filter { case (_, ((id1, _), (id2, _))) => id1 < id2 }

            ratingPairs.map { case (_, ((id1, value1), (id2, value2))) => ((id1, id2), (value1, value2)) }
        }

        private def isRelatedSimilarity(itemId: Int)(itemToItemSimilarity: ((Int, Int), Similarity)) = {
            val ((id1, id2), similarity) = itemToItemSimilarity
            val isRelated = id1 == itemId || id2 == itemId
            val isStrongEnough = similarity.strength > minSimilarityStrength && similarity.value > minSimilarityValue

            isRelated && isStrongEnough
        }

        private def isRelatedSimilarity(itemIds: Array[Int])(itemToItemSimilarity: ((Int, Int), Similarity)) = {
            val ((id1, id2), similarity) = itemToItemSimilarity
            val isRelated = itemIds.contains(id1) || itemIds.contains(id2)
            val isStrongEnough = similarity.strength > minSimilarityStrength && similarity.value > minSimilarityValue

            isRelated && isStrongEnough
        }

        private def keepOtherSimilarity(itemId: Int)(itemToItemSimilarity: ((Int, Int), Similarity)) = {
            val ((id1, id2), similarity) = itemToItemSimilarity

            (if (id1 == itemId) id2 else id1, similarity)
        }

        private def keepOtherSimilarity(itemIds: Array[Int])(itemToItemSimilarity: ((Int, Int), Similarity)) = {
            val ((id1, id2), similarity) = itemToItemSimilarity

            (if (itemIds.contains(id1)) id2 else id1, similarity)
        }
    }

}

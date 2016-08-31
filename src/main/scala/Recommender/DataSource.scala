package Recommender

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object DataSource {
    def items(from: DataSource, idPosition: Int, namePosition: Int)(implicit sc: SparkContext): RDD[Serialized.Item] = {
        from match {
            case File(path, delimeter) => {
                val fields = sc.textFile(path).map(_.split(delimeter))
                fields.map(field => Item(field(idPosition).toInt, field(namePosition)).serialize)
            }
        }
    }

    def userRatings(from: DataSource, userIdPosition: Int, itemIdPosition: Int, valuePosition: Int)(implicit sc: SparkContext): RDD[Serialized.UserRating] = {
        from match {
            case File(path, delimeter) => {
                val fields = sc.textFile(path).map(_.split(delimeter))
                fields.map(
                    field => UserRating(User(field(userIdPosition).toInt),
                        Rating(field(itemIdPosition).toInt, field(valuePosition).toDouble)).serialize)
            }
        }
    }
}

class DataSource

case class File(path: String, delimeter: String) extends DataSource
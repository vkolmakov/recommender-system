package Recommender

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object DataSource {
    def items(from: DataSource, idPosition: Int, namePosition: Int)(implicit sc: SparkContext): RDD[Types.Item] = {
        from match {
            case File(path, delimeter) => {
                val fields = sc.textFile(path).map(_.split(delimeter))
                fields.map(field => (field(idPosition).toInt, field(namePosition)))
            }
        }
    }
    def ratings(from: DataSource, userIdPosition: Int, itemIdPosition: Int, valuePosition: Int)(implicit sc: SparkContext): RDD[Types.UserRating] = {
        from match {
            case File(path, delimeter) => {
                val fields = sc.textFile(path).map(_.split(delimeter))
                fields.map(field => (field(userIdPosition).toInt, (field(itemIdPosition).toInt, field(valuePosition).toDouble)))
            }
        }
    }
}

class DataSource
case class File(path: String, delimeter: String) extends DataSource
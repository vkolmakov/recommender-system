package Recommender

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset

object DataSource {
    def items(from: DataSource, idPosition: Int, namePosition: Int)(implicit spark: SparkSession): Dataset[Item] = {
        import spark.implicits._
        from match {
            case File(path, delimeter) => {
                spark.read.text(path).map(_.split(delimeter))
                    .map(field => Item(field(idPosition).toInt, field(namePosition)))
            }
        }
    }
    def userRatings(from: DataSource, userIdPosition: Int, itemIdPosition: Int, valuePosition: Int)(implicit spark: SparkSession): Dataset[(User, Rating)] = {
        import spark.implicits._
        from match {
            case File(path, delimeter) => {
                spark.read.text(path).map(_.split(delimeter))
                    .map(field =>
                      (User(field(userIdPosition).toInt),
                       Rating(field(itemIdPosition).toInt, field(valuePosition).toDouble)))
            }
        }
    }
}

class DataSource
case class File(path: String, delimeter: Char) extends DataSource
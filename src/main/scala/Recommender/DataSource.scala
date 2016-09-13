package Recommender

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset

object DataSource {
    def items(from: DataSource)(implicit spark: SparkSession): Dataset[Item] = {
        import spark.implicits._
        from match {
            case File(path, delimeter, config) => {
                val itemConfig = config.get("items")
                val (idPosition, namePosition) = itemConfig match {
                    case Some(conf) =>
                        (conf.get("idPosition") match { case Some(pos) => pos },
                         conf.get("namePosition") match { case Some(pos) => pos })
                    case _ => throw new IllegalArgumentException
                }
                spark.read.text(path).map(_.split(delimeter))
                    .map(field => Item(field(idPosition).toInt, field(namePosition)))
            }
        }
    }
    def userRatings(from: DataSource)(implicit spark: SparkSession): Dataset[(User, Rating)] = {
        import spark.implicits._
        from match {
            case File(path, delimeter, config) => {
                val userRatingsConfig = config.get("userRatings")
                val (userIdPosition, itemIdPosition, valuePosition) = userRatingsConfig match {
                    case Some(conf) =>
                        (conf.get("userIdPosition") match { case Some(pos) => pos },
                         conf.get("itemIdPosition") match { case Some(pos) => pos },
                         conf.get("valuePosition") match { case Some(pos) => pos })
                    case _ => throw new IllegalArgumentException
                }
                spark.read.text(path).map(_.split(delimeter))
                    .map(field =>
                      (User(field(userIdPosition).toInt),
                       Rating(field(itemIdPosition).toInt, field(valuePosition).toDouble)))
            }
        }
    }
}

class DataSource
case class File(path: String, delimeter: Char, config: Map[String, Map[String, Int]]) extends DataSource
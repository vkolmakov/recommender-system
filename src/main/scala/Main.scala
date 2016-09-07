import Recommender.{Recommender, File}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Main extends App {
    Logger.getLogger("org").setLevel(Level.ERROR)

    implicit val spark = SparkSession
        .builder()
        .appName("Recommender")
        .master("local[*]")
        .getOrCreate()

    val itemsSource = File("./data/ml-100k/u.item", '|')
    val userRatingsSource = File("./data/ml-100k/u.data", '\t')
    val recommender = Recommender(itemsSource, userRatingsSource)
    recommender.recommendationsFor(50).foreach(println)

    spark.stop()
}

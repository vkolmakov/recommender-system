import Recommender.{File, Recommender, Item, User}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Main extends App {
    Logger.getLogger("org").setLevel(Level.ERROR)

    implicit val spark = SparkSession
        .builder()
        .appName("Recommender")
        .master("local[*]")
        .getOrCreate()

    val itemSource = File("./data/ml-100k/u.item", '|')
    val userRatingSource = File("./data/ml-100k/u.data", '\t')
    val recommender = Recommender(itemSource, userRatingSource)

    val selectedItem: Item = Item(50, "Star Wars")
    val selectedUser: User = User(0)

    recommender.recommendationsFor(selectedUser).foreach(println)
    recommender.recommendationsFor(selectedItem).foreach(println)

    spark.stop()
}

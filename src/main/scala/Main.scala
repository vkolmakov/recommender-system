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

    val dataSourceConfig = Map(
        "items" -> Map(
            "idPosition" -> 0,
            "namePosition" -> 1
        ),
        "userRatings" -> Map(
            "userIdPosition" -> 0,
            "itemIdPosition" -> 1,
            "valuePosition" -> 2
        )
    )

    val itemSource = File("./data/ml-100k/u.item", '|', dataSourceConfig)
    val userRatingSource = File("./data/ml-100k/u.data", '\t', dataSourceConfig)

    val recommender = Recommender(itemSource, userRatingSource)

    val selectedItem: Item = Item(50, "Star Wars")
    val selectedUser: User = User(0)

    recommender.recommendationsForUser(selectedUser).foreach(println)
    recommender.recommendationsForItem(selectedItem).foreach(println)

    spark.stop()
}

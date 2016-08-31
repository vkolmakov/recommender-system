package Recommender


object Serialized {
    type User = Int
    type Item = (Int, String)
    type Rating = (Int, Double)
    type UserRating = (User, Rating)
    type Similarity = (Double, Int)
}

case class User(id: Int) {
    def serialize: Serialized.User = id
    def deserialize(user: Serialized.User): User = User(user)
}

case class Item(id: Int, name: String) {
    def serialize: Serialized.Item = (id, name)
    def deserialize(item: Serialized.Item): Item = Item(item._1, item._2)
}

case class Rating(itemId: Int, value: Double) {
    def serialize: Serialized.Rating = (itemId, value)
    def deserialize(rating: Serialized.Rating): Rating = Rating(rating._1, rating._2)
}

case class UserRating(user: User, rating: Rating) {
    def serialize: Serialized.UserRating = (user.serialize, rating.serialize)
    def deserialize(userRating: Serialized.UserRating): UserRating =
        UserRating(User(userRating._1), Rating(userRating._2._1, userRating._2._2))
}

case class Similarity(value: Double, strength: Int) {
    def serialize: Serialized.Similarity = (value, strength)
    def deserialize(similarity: Serialized.Similarity): Similarity =
        Similarity(similarity._1, similarity._2)
}
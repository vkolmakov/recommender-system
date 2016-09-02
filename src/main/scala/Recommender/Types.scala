package Recommender

final case class User(id: Int)
final case class Item(id: Int, name: String)
final case class Rating(itemId: Int, value: Double)
final case class Similarity(value: Double, strength: Int)

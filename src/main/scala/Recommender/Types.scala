package Recommender


object Types {
    type User = Int
    type Item = (Int, String)
    type Rating = (Int, Double)

    type UserRating = (User, Rating)
}

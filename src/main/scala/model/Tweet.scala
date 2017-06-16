package model

import java.util.Date

import play.api.libs.json.Json

/**
  * Created by inti on 11/06/17.
  */
case class TweetInfo (
                       id: Long,
                       user: String,
                       sentence: String,
                       country: String,
                       lang: String,
                       retweetCount: Int,
                       isRetweet: Boolean,
                       eventDate: String,
                       keySearch: String
                     )

case class TweetWithAdds (
                           id: Long,
                           user: String,
                           text: String,
                           country: Option[String],
                           lang: String,
                           retweetCount: Int,
                           isRetweet: Boolean,
                           eventDate: String,
                           proba: Double,
                           predict: Double,
                           keySearch: String
                       )

case class TweetWithAddsAndDate (
                           id: Long,
                           lang: String,
                           retweetCount: Int,
                           isRetweet: Boolean,
                           eventDate: String,
                           proba: Double,
                           predict: Double,
                           keySearch: String,
                           country: String
                         )

object TweetInfo {
  implicit val tweetFormat = Json.format[TweetInfo]
}
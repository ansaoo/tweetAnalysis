package utils

import model.TweetInfo
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import play.api.libs.json.{JsError, JsSuccess, Json}
import twitter4j.Status


/**
  * Created by inti on 16/06/17.
  */
object TweetUtils {

  def parseTweet(received: Status, keysSearch: Array[String]) = {
    try {
      val country = getCountry(received)
      val now = getDateTime()
      val isRetweet = isRetweeted(received)
      val keySearch = getSearchKey(received.getText, keysSearch)
      Some(TweetInfo(
        received.getId,
        received.getUser.getName,
        received.getText,
        country,
        received.getLang,
        received.getRetweetCount,
        isRetweet,
        now,
        keySearch(0)
      ))
    }
    catch {
      case e: Exception => None
    }
  }

  def getSearchKey(textTweet: String, keysSearch: Array[String]): Array[String] = {
    keysSearch.flatMap{
      case word if (textTweet.contains(word)) => Some(word)
      case word => None
      }
    }

  def getDateTime() = {
    val now = new DateTime()
    ISODateTimeFormat.dateTimeParser().parseDateTime(now.toString()).toString()
  }

  def stringToTweet(consumerValue: String): Option[TweetInfo] = {
    Json.parse(consumerValue).validate[TweetInfo] match {
      case JsError(e) => println(e); None
      case JsSuccess(t, _) => Some(t)
    }
  }

  def getCountry(received: Status) = {
    try {
      received.getPlace.getCountry
    }
    catch {
      case e: Exception => {
        "non"
      }
    }
  }

  def isRetweeted(received: Status) = {
    try {
      received.getRetweetedStatus.getId
      true
    }
    catch {
      case e: Exception => false
    }
  }

}

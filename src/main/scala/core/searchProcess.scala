package core

import com.danielasfregola.twitter4s.TwitterClient
import com.danielasfregola.twitter4s.entities.{AccessToken, ConsumerToken}
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by inti on 12/06/17.
  */
object searchProcess {

  val consumerKey = "3Ax78wxvjlrboTUXx5eHnWayU"
  val consumerSecret = "GVo6WDH1ay3vhuAeosICCskCQ1nV2VQN62ZZagqUV6DLRSn7Ah"
  val accessTokenKey = "860356285191028736-DwOiGkvW1LGocxRweFnpS9XtlBNLEzM"
  val accessTokenSecret = "JqeLKXi3Ej2o6dPhYiXAJVRmASRQc73P71ODKvkMFfEmb"

  def loadSearch(key: String) = {

    val consumerToken = ConsumerToken(key = consumerKey, secret = consumerSecret)
    val accessToken = AccessToken(key = accessTokenKey, secret = accessTokenSecret)
    val client = new TwitterClient(consumerToken, accessToken)

    //  client.getUserTimelineForUser(screen_name = "odersky", count = 200)

    val search = client.searchTweet(key, count = 100)
    search.map(response => response.statuses)
      .map(listeTweets => {
        val frTweets = listeTweets.filter(_.lang == Some("fr"))
        frTweets.foreach {
          x => println(x.text)
        }
      }
      )
  }

}

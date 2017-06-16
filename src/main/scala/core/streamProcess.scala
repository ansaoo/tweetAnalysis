package core

import core.mlProcess.{predictTweets}
import model.TweetWithAddsAndDate
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset}
import org.apache.spark.streaming.dstream.{ReceiverInputDStream}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.elasticsearch.spark.sql._
import twitter4j.Status
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import utils.TweetUtils._

/**
  * Created by inti on 09/06/17.
  */
object streamProcess {

  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("TweetAnalysis")
  val ssc = new StreamingContext(conf, Seconds(5))



  def loadStream(keysSearch: Array[String], accessConfig: Array[String])= {

//    val consumerKey = "3Ax78wxvjlrboTUXx5eHnWayU"
//    val consumerSecret = "GVo6WDH1ay3vhuAeosICCskCQ1nV2VQN62ZZagqUV6DLRSn7Ah"
//    val accessTokenKey = "860356285191028736-DwOiGkvW1LGocxRweFnpS9XtlBNLEzM"
//    val accessTokenSecret = "JqeLKXi3Ej2o6dPhYiXAJVRmASRQc73P71ODKvkMFfEmb"

    val consumerKey = accessConfig(0)
    val consumerSecret = accessConfig(1)
    val accessTokenKey = accessConfig(2)
    val accessTokenSecret = accessConfig(3)


    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessTokenKey)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val searchList = Array("obama","poutine","macron","trump")
    val auth = Some(new OAuthAuthorization(new ConfigurationBuilder().build()))

    val stream: ReceiverInputDStream[Status] = TwitterUtils.createStream(ssc, auth, keysSearch)

    stream.flatMap(parseTweet(_, keysSearch))
      .filter(tweet => tweet.lang.equals("en"))
      .foreachRDD(rdd => {
        val dfTweet: Dataset[TweetWithAddsAndDate] = predictTweets(rdd)
        dfTweet.saveToEs("tweets/tweet")
      })

    ssc.start()
    ssc.awaitTermination()
  }
}

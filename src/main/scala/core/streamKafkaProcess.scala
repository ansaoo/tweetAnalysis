package core

import model.TweetWithAddsAndDate
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.elasticsearch.spark.sql._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.TweetUtils._
import core.mlProcess._
import org.apache.spark.sql.Dataset

import scala.io.Source

/**
  * Created by inti on 13/06/17.
  */
object streamKafkaProcess extends App {

  val config: Array[String] = Source.fromFile("config").getLines()
    .filter(line => !line.startsWith("#"))
    .toArray

  val conf = new SparkConf().setMaster("local[*]")
    .setAppName("SparkEs")
    .set("es.index.auto.create", "true")
    .set("es.nodes", "localhost")
    .set("es.port", "9200")
  val ssc = new StreamingContext(conf, Seconds(10))

  //  val reportScan = new Report(sc.longAccumulator("Accumulator Scan Success"), sc.longAccumulator("Accumulator Scan Error"))
  //  val reportStore = new Report(sc.longAccumulator("Accumulator Store Success"), sc.longAccumulator("Accumulator Store Error"))

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "group.id" -> "use_a_separate_group_id_for_each_stream",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )
  val topics = Array(config(6))
  val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
    ssc,
    PreferConsistent,
    Subscribe[String, String](topics, kafkaParams)
  )

  println("Load streaming...")

  kafkaStream.map(_.value())
    .flatMap(stringToTweet(_))
    .foreachRDD(rdd => {
      val dfTweet: Dataset[TweetWithAddsAndDate] = predictTweets(rdd)
      dfTweet.saveToEs("tweets/tweet")
    })

  ssc.start()
  ssc.awaitTermination()

}

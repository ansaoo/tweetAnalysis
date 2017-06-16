package core

import model.{TweetInfo, TweetWithAddsAndDate}
import org.apache.spark.ml.{PipelineModel}
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import utils.ML._
/**
  * Created by inti on 12/06/17.
  */
object mlProcess {

  def createAndSaveModel() = {
    val sp = SparkSession.builder()
      .appName("SparkSession")
      .master("local[*]")
      .getOrCreate()
    val filesPos = getListOfFiles("src/main/resources/txt_sentoken/pos")
    val filesNeg = getListOfFiles("src/main/resources/txt_sentoken/neg")

    val loadPos = getLinesOfFiles(1.0, filesPos)
    val loadNeg = getLinesOfFiles(0.0, filesNeg)

    val mergedData = loadPos ::: loadNeg

    val sentenceData = sp.createDataFrame(mergedData).toDF("id", "text", "label")
    val Array(trainingData, testData) = sentenceData.randomSplit(Array(0.7, 0.3), seed = 1234L)

    val model = createPipeline().fit(trainingData)
    val predictions = model.transform(testData)
    // Select (prediction, true label) and compute test error
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    model.write.overwrite().save("mlModel")
    accuracy
  }

  def modelPrediction(testDf: DataFrame) = {
    // Make predictions on test documents. cvModel uses the best model found (lrModel)
    val model = try {
      PipelineModel.load("mlModel")
    }
    catch {
      case e: Exception => {
        createAndSaveModel()
        PipelineModel.load("mlModel")
      }
    }
//    val model = PipelineModel.load("mlModel")
    model.transform(testDf)
      .select("id", "isRetweet", "eventDate", "lang", "retweetCount", "keySearch", "probability", "prediction", "country")
  }
  def predictTweets(rddTweet: RDD[TweetInfo]) = {
    val sp = SparkSession.builder()
      .appName("SparkSession")
      .master("local[*]")
      .getOrCreate()
    import sp.implicits._
    val dfTweet = rddTweet.toDF()
    val now = new DateTime()
    val nowISO = ISODateTimeFormat.dateTimeParser.parseDateTime(now.toString).toString()
    val prediction = modelPrediction(dfTweet)
        .map{ case Row(id: Long, isRetweet: Boolean, eventDate: String, lang: String, retweetCount: Int, keySearch: String, prob: Vector, prediction: Double, country: String) =>
          val proba = if (prob.apply(0).<=(prob.apply(1))) {
            prob.apply(1)
          }
          else {
            prob.apply(0)
          }
          TweetWithAddsAndDate(id,lang,retweetCount,isRetweet,eventDate,proba,prediction,keySearch,country)
        }
    prediction.write.json(s"data/tweet_$nowISO")
    prediction
  }
}

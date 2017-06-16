package utils


import java.io.File

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.feature.{HashingTF, IDF, IDFModel, Tokenizer}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.Source

/**
  * Created by inti on 16/06/17.
  */
object ML {

  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def getLinesOfFiles(ind: Double, path: List[File]): List[(Int, String, Double)] = {
    path.flatMap{file =>
      Source.fromFile(file).getLines()
        .zipWithIndex
        .map(x => (x._2, x._1, ind))
    }
  }

  def transformData(sp: SparkSession, sentenceData: DataFrame): DataFrame = {
    val tokenizer = new Tokenizer()
      .setInputCol("sentence")
      .setOutputCol("words")
    val hashingTF = new HashingTF()
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("rawFeatures")
    val idf = new IDF()
      .setInputCol(hashingTF.getOutputCol)
      .setOutputCol("features")
    val wordsData: DataFrame = tokenizer.transform(sentenceData)
    val featurizedData: DataFrame = hashingTF.transform(wordsData)
    val idfModel: IDFModel = idf.fit(featurizedData)
    idfModel.transform(featurizedData)
  }

  def createPipeline(): Pipeline = {
    val tokenizer = new Tokenizer()
      .setInputCol("sentence")
      .setOutputCol("words")
    val hashingTF = new HashingTF()
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")
    val model = new NaiveBayes()
    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, model))
    pipeline
  }
}

name := "tweetAnalysis"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0" % "provided"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.1.1"
libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.1.0"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.1.1"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.1.1"
libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-20" % "5.4.1"
libraryDependencies += "com.typesafe.play" % "play-json_2.11" % "2.4.0"
libraryDependencies += "com.danielasfregola" %% "twitter4s" % "0.2.1"
libraryDependencies += "joda-time" % "joda-time" % "2.9.9"

resolvers ++= Seq(
    "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
    "Confluent" at "http://packages.confluent.io/maven/"
)
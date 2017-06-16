import java.util.Date

import core.streamProcess._
import scala.io.Source

object Boot {

  def main(args: Array[String]): Unit = {
    val config: Iterator[(String, Int)] = Source.fromFile("config").getLines()
      .filter(line => !line.startsWith("#"))
      .zipWithIndex
    val accessConfig: Array[String] = config.take(4).map(x => x._1).toArray
    val searchList: Array[String] = config.filter(_._2==4).flatMap(tuple => tuple._1.split(",")).toArray
    loadStream(searchList, accessConfig)

  }
}
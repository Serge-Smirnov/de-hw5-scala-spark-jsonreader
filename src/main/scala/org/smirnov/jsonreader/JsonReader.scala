package org.smirnov

import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
//import org.json4s.native.JsonMethods._


object JsonReader extends App {
  case class Wine(id:Option[Int], country:Option[String], points:Option[Int], price:Option[Double], title:Option[String], variety: Option[String], winery:Option[String])
  implicit val formats: Formats = DefaultFormats
  //  implicit val formats = {
  //    Serialization.formats(FullTypeHints(List(classOf[Wine])))
  //  }

  val spark = SparkSession.builder().master("local").getOrCreate()
  val sc = spark.sparkContext
  val filename = args(0)

  val res = sc
    .textFile(filename)
    .foreach(s => println(parse(s).extract[Wine]))
}

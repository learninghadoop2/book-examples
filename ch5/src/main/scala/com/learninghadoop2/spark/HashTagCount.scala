package com.learninghadoop2.spark

import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import scala.util.matching.Regex

object HashTagCount {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: HashtagCount <master> <input> <output>")
      System.err.println(args.length)
      System.err.println(args.toString) 
      System.exit(1)
    }

    val master = args(0).toString
    val inputFile = args(1).toString
    val outputPath = args(2).toString

    val sc = new SparkContext(master, "HashtagCount", System.getenv("SPARK_HOME"))

    val file = sc.textFile(inputFile)
    val pattern = new Regex("(?:\\s|\\A|^)[##]+([A-Za-z0-9-_]+)")
    
    val counts = file.flatMap(line => (pattern findAllIn line).toList)
      .map(word => (word, 1))
      .reduceByKey( (m, n) => m+n)  
    counts.saveAsTextFile(outputPath)

    System.exit(0)
  }
}

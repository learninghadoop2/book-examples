package com.learninghadoop2.spark;

import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import scala.util.matching.Regex
import scala.io.Source


object HashTagSentiment {
  def sentimentScore(str: String, positive: Set[String], negative: Set[String]): (Double, Int) = {
    var positiveScore = 0; var negativeScore = 0;
        str.split("""\s+""").foreach { w =>
          if (positive.contains(w)) { positiveScore+=1; }
          if (negative.contains(w)) { negativeScore+=1; }
        } 
        ((positiveScore - negativeScore).toDouble, str.split("""\s+""").length)
  }


  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: HashtagSentiment <master> <input> <output> <positive words> <negative words>")
      System.err.println(args.length )
      System.err.println(args.toString ) 
      System.exit(1)
    }

    var master = args(0).toString
    var inputFile = args(1).toString
    var outputPath = args(2).toString
    var positiveWordsPath = args(3).toString
    var negativeWordsPath = args(4).toString

    val sc = new SparkContext(master, "HashtagSentiment", System.getenv("SPARK_HOME"))

    val file = sc.textFile(inputFile)

    // Read a list of positive and negative works into a Swt and filter out comments
    val positive = Source.fromFile(positiveWordsPath).getLines.filterNot(_ startsWith ";").toSet
    val negative = Source.fromFile(negativeWordsPath).getLines.filterNot(_ startsWith ";").toSet

    val pattern = new Regex("(?:\\s|\\A|^)[##]+([A-Za-z0-9-_]+)")

    val counts = file.flatMap(line => (pattern findAllIn line).map({
        word => (word, sentimentScore(line, positive, negative)) 
      })).reduceByKey({ (m, n) => (m._1 + n._1, m._2 + n._2) })
    
    val sentiment = counts.map({hashtagScore =>
        val hashtag = hashtagScore._1
        val score = hashtagScore._2
        val normalizedScore = score._1 / score._2
        (hashtag, normalizedScore)
    })
    

    sentiment.saveAsTextFile(outputPath)

  }
}

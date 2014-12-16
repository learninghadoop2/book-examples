package com.learninghadoop2.spark;

import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.twitter._
import scala.util.matching.Regex
import scala.io.Source


object StreamingHashTagSentiment {
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
      System.err.println("Usage: StreamingHashtagSentimentCount <master> <positive words> <negative words> <window>")
      System.err.println(args.length )
      System.err.println(args.toString ) 
      System.exit(1)
    }

    val auth = None

    val master = args(0).toString

    val positiveWordsPath = args(1).toString
    val negativeWordsPath = args(2).toString

    val window = args(3).toLong

    val updateFunc = (values: Seq[Double], state: Option[Double]) => {
      val decayFactor = 0.9
      val currentScore = values.sum

      val previousScore = state.getOrElse(0.0)

      Some( (currentScore + previousScore) * decayFactor)
    }

    val ssc = new StreamingContext(master, "HashtagSentimentTrend", Seconds(window))


    // Read a list of positive and negative words and filter out comments
    val positive = Source.fromFile(positiveWordsPath)
      .getLines
      .filterNot(_ startsWith ";")
      .toSet
    val negative = Source.fromFile(negativeWordsPath)
      .getLines
      .filterNot(_ startsWith ";")
      .toSet

    val pattern = new Regex("(?:\\s|\\A|^)[##]+([A-Za-z0-9-_]+)")

    val stream = TwitterUtils.createStream(ssc, auth)

    val text = stream.map(tweet => tweet.getText())
    val counts = text.flatMap(line => (pattern findAllIn line)
      .toList
      .map(word => (word, sentimentScore(line, positive, negative))))
      .reduceByKey({ (m, n) => (m._1 + n._1, m._2 + n._2) })

    val sentiment = counts.map({hashtagScore =>
        val hashtag = hashtagScore._1
        val score = hashtagScore._2
        val normalizedScore = score._1 / score._2
        (hashtag, normalizedScore)
    })
    
    val stateDstream = sentiment.updateStateByKey[Double](updateFunc)

    stateDstream.print

    ssc.checkpoint("/tmp/checkpoint")
    ssc.start()
  }
}

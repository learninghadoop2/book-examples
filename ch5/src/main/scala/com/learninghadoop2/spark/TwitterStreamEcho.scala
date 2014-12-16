package com.learninghadoop2.spark;
import org.apache.spark.streaming.{Seconds, StreamingContext}
import StreamingContext._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._


object TwitterStreamEcho {
   def main(args: Array[String]) {
    if (args.length < 2) {
          System.err.println("Usage: TwitterStreamEcho <master> <window length>")
          System.err.println(args.length )
          System.err.println(args.toString ) 
          System.exit(1)
    }
    val auth = None
    
    val master = args(0)
    val window = args(1).toLong

    val ssc = new StreamingContext(master, "TwitterStreamEcho", Seconds(window),
            System.getenv("SPARK_HOME"))

    val stream = TwitterUtils.createStream(ssc, auth)

    val tweets = stream.map(tweet => (tweet.getText()))
    tweets.print()

    ssc.start()
    ssc.awaitTermination()
  }
}

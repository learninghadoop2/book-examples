package com.learninghadoop2.spark

import org.apache.spark.streaming.{Seconds, StreamingContext}
import StreamingContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf

import com.google.gson.Gson

import twitter4j._

import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import org.apache.spark.streaming.twitter.TwitterUtils

object SqlOnStream {
    private var gson = new Gson()

    def main(args: Array[String]) {
        if (args.length < 2) {
          System.err.println("Usage: SqlOnStream <master> <window length>")
          System.err.println(args.length )
          System.err.println(args.toString ) 
          System.exit(1)
        }
        val master = args(0)
        val window = args(1).toLong

	/* TODO: 
	   passing a None auth to TwitterUtils.createStream will break JSON
	   serialization. To run this example, manually instantiate an 
	   OAuthAuthorization object */
        val auth = Some( new OAuthAuthorization(
            new twitter4j.conf.ConfigurationBuilder()
               .setOAuthConsumerKey("")
               .setOAuthConsumerSecret("")
               .setOAuthAccessToken("")
               .setOAuthAccessTokenSecret("")
               .build
        ))

        val conf = new SparkConf().setAppName("Execute an SQL query on a DStream").setMaster(master)

        val sc = new SparkContext(conf)

        val ssc = new StreamingContext(sc, Seconds(window))

        /* Serialize each Tweet object to a JSON string */
        val dstream = TwitterUtils.createStream(ssc, auth).map(gson.toJson(_))

        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        import sqlContext._

        dstream.foreach( rdd => {
            val json_rdd = sqlContext.jsonRDD(rdd)
            json_rdd.registerTempTable("tweets")
            json_rdd.printSchema 

            val res = sqlContext.sql("select text from tweets")
            res.foreach(println)
        })

        ssc.checkpoint("/tmp/checkpoint")
        ssc.start() 
        ssc.awaitTermination() 
    }

}

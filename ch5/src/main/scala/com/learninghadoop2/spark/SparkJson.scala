package com.learninghadoop2.spark;

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.util.matching.Regex

import com.google.gson.Gson


object SparkJson {
    def main(args: Array[String]) {
        if (args.length < 2) {
          System.err.println("Usage: SparkJson <master> <input file>")
          System.err.println(args.length )
          System.err.println(args.toString ) 
          System.exit(1)
        }
        //val master = "spark://localhost:7077"
        val master = args(0)
        val inFile = args(1)

        val conf = new SparkConf().setAppName("Simple Application").setMaster(master)
        
        val sc = new SparkContext(conf)
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        import sqlContext._

        val tweets = sqlContext.jsonFile(inFile)

        tweets.registerTempTable("tweets")
        tweets.printSchema

        val text = sqlContext.sql("SELECT text, user.id FROM tweets")

       // Find the ten most popular hashtags hashtags
       val pattern = new Regex("(?:\\s|\\A|^)[##]+([A-Za-z0-9-_]+)")
    
       val counts = text.flatMap(sqlRow => (pattern findAllIn sqlRow(0).toString).toList)
         .map(word => (word, 1))
         .reduceByKey( (m, n) => m + n)
       
       counts.registerTempTable("hashtag_frequency")

       counts.printSchema

       val top10 = sqlContext.sql("SELECT _1 as hashtag, _2 as frequency FROM hashtag_frequency order by frequency desc limit 10")

       top10.foreach(println)

      }
}

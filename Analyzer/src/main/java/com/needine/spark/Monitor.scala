package com.needine.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import scala.concurrent.duration._

object Monitor {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder()
      .appName("Structured Streaming Network Traffic Analysis")
      .config("spark.master", "local[*]")
      .getOrCreate()
   
    import spark.implicits._
    import org.apache.spark.sql.Encoders    
    import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
    import org.apache.spark.sql.execution.debug._
    import org.apache.spark.sql.functions._
    
    spark.sparkContext.setLogLevel("WARN")
       
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092,anotherhost:9092")
      .option("subscribe", "connect-test")
      .load()
      //.withWatermark(eventTime = "timestamp", delayThreshold = "10 seconds")
      
    //df.printSchema()  
    //val lines = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)].map(_._2)
    val lines = df.selectExpr("CAST(timestamp AS TIMESTAMP)", "CAST(value AS STRING)")//.select($"timestamp", $"value").withColumn("unix_arrival", unix_timestamp($"timestamp")).withColumn("unix_time_now", unix_timestamp)
    //lines.printSchema()
    
    val query = lines
//      .groupBy("value").count()
      //.groupBy(window($"timestamp", "10 minutes", "2 minutes"),$"value").count()
      //.groupBy(window($"timestamp", "2 seconds", "2 seconds"),$"unix_arrival" ,$"value").count()
      .withWatermark("timestamp", "20 seconds")
      .groupBy(window($"timestamp", "10 seconds"),$"value").count().sort($"window")
      .writeStream
//      .trigger(Trigger.ProcessingTime(10.seconds))
      .outputMode("complete")
      //.outputMode("append")
      .format("console")
      .start()
      
    /*
    val query = df
      .writeStream
      .outputMode("append")
      .format("console")
      .start()        
    */  
    query.awaitTermination()
    
    //Needine.com
    //Network Traffic Status V0.2
  }
}
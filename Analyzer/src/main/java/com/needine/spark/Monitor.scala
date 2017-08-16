package com.needine.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import scala.concurrent.duration._
import java.sql.Struct
//import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row

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
      
    val lines = df.selectExpr("CAST(timestamp AS TIMESTAMP)", "CAST(value AS STRING)")//.select($"timestamp", $"value").withColumn("unix_arrival", unix_timestamp($"timestamp")).withColumn("unix_time_now", unix_timestamp)
    //lines.printSchema()
    
    val query = lines
      .withWatermark("timestamp", "20 seconds")
      .groupBy(window($"timestamp", "10 seconds"),$"value").count().withColumn("end", $"window.end")
      .select("value", "count", "end" )
      .selectExpr("CAST(value AS STRING)", "CAST(count AS Long)" ,"CAST(end AS TIMESTAMP)" ).withColumn("end2", unix_timestamp($"end"))
      .filter($"end2".gt(unix_timestamp - 20))
      .sort(desc("end"))//.limit(5)
      .writeStream
//      .trigger(Trigger.ProcessingTime(10.seconds))
      .outputMode("complete")
      //.outputMode("append")
      .format("console")
      .start()
      
    query.awaitTermination()
    
    //Needine.com
    //Network Traffic Status V0.2
  }
}
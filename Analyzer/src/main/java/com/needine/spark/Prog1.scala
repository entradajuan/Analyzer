package com.needine.spark

import org.apache.spark.sql.SparkSession

object Prog1 {
  case class Trace (ori: String, des: String, pro: String)
    
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
    val lines = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]

    val packetsDS = lines.map{cad => Trace(cad._2.split("→")(0).split(" ").last, cad._2.split("→")(1).split(" ")(1), cad._2.split("→")(1).split(" ")(2))}//.select($"ori")
    packetsDS.printSchema()
    
    val query = packetsDS.groupBy($"ori", $"des", $"pro").count().orderBy(-$"count")
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()
    
    query.awaitTermination()

    //println("Needine.com\nNetwork Traffic Status V0.2")
  }
  
}
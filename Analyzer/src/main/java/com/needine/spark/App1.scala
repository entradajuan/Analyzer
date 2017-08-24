package com.needine.spark

import org.apache.spark.sql.SparkSession

object App1 {
  case class Trace (arrival: String, ori: String, des: String, pro: String, bytes: String)
  
  
  def cleansingArrival(cad: String): String = {
    if (cad == null) {return "NADA"}
    if (cad.split("→").length < 1) {return "NADA"}
    if (cad.split("→")(0).split(" +").length < 1) {return "NADA"}
    if (cad.split("→")(0).split(" +").last == "") {return "NADA"}
    cad.split("→")(0).split(" +")(1)+";"
  }
  
  def cleansingOri(cad: String): String = {
    if (cad == null) {return "NADA"}
    if (cad.split("→").length < 1) {return "NADA"}
    if (cad.split("→")(0).split(" +").length < 1) {return "NADA"}
    if (cad.split("→")(0).split(" +").last == "") {return "NADA"}
    cad.split("→")(0).split(" +").last+";"
  }

  def cleansingDest(cad: String): String = {
    if (cad == null) {return "NADA"}
    if (cad.split("→").length < 2) {return "NADA"}
    if (cad.split("→")(1).split(" +").length < 2) {return "NADA"}
    if (cad.split("→")(1).split(" +").last == "") {return "NADA"}
    cad.split("→")(1).split(" +")(1)+";"
  }
  
  def cleansingProt(cad: String): String = {
    if (cad == null) {return "NADA"}
    if (cad.split("→").length < 2) {return "NADA"}
    if (cad.split("→")(1).split(" +").length < 2) {return "NADA"}
    if (cad.split("→")(1).split(" +")(2) == "") {return "NADA"}
    cad.split("→")(1).split(" +")(2)+";"
  }

  def cleansingBytes(cad: String): String = {
    if (cad == null) {return "NADA"}
    if (cad.split("→").length < 2) {return "NADA"}
    if (cad.split("→")(1).split(" +").length < 2) {return "NADA"}
    if (cad.split("→")(1).split(" +")(3) == "") {return "NADA"}
    cad.split("→")(1).split(" +")(3)+";"
  }
  
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

    val packetsDS = lines.map{cad => Trace(cleansingArrival(cad._2), cleansingOri(cad._2), cleansingDest(cad._2), cleansingProt(cad._2),cleansingBytes(cad._2))}
    
    val query = packetsDS.toDF.withColumn("x", concat($"arrival", $"ori",  $"des", $"pro", $"bytes"))
      .select($"ori" as "key", $"x" as "value")
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092,anotherhost:9092")
      .option("topic", "cleanedData")
      .option("checkpointLocation", "/tmp/kafkaSink")
      .start()

    query.awaitTermination()
    
    //Needine.com
    //Network Traffic Status V0.2
  }
}
  
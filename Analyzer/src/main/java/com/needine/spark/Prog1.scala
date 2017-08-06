package com.needine.spark

import org.apache.spark.sql.SparkSession

object Prog1 {
  case class Trace (ori: String, des: String, pro: String, bytes: Int)
   
  def cleasingOri(cad: String): String = {
    if (cad == null) {return "NADA"}
    if (cad.split("→").length < 1) {return "NADA"}
    if (cad.split("→")(0).split(" +").length < 1) {return "NADA"}
    if (cad.split("→")(0).split(" +").last == "") {return "NADA"}
    cad.split("→")(0).split(" +").last+";;"
  }

  def cleasingDest(cad: String): String = {
    if (cad == null) {return "NADA1"}
    if (cad.split("→").length < 2) {return "NADA2"}
    if (cad.split("→")(1).split(" +").length < 2) {return "NADA3"}
    if (cad.split("→")(1).split(" +").last == "") {return "NADA4"}
    cad.split("→")(1).split(" +")(1)+";;"
  }

  
  def cleasingProt(cad: String): String = {
    if (cad == null) {return "NADA"}
    if (cad.split("→").length < 2) {return "NADA"}
    if (cad.split("→")(1).split(" +").length < 2) {return "NADA"}
    if (cad.split("→")(1).split(" +")(2) == "") {return "NADA"}
    cad.split("→")(1).split(" +")(2)+";;"
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

    
    //val packetsDS = lines.map{cad => Trace(cad._2.split("→")(0).split(" ").last, cad._2.split("→")(1).split(" ")(1), cad._2.split("→")(1).split(" ")(2), 5)}//.select($"ori")
    
    
    //val packetsDS = lines.map{cad => Trace(checkOri(cad._2.split("→")(0).split(" ").last), checkDest(cad._2.split("→")(1).split(" ")(1)), cad._2.split("→")(1).split(" ")(2),5/*, checkBytes(cad._2.split("→")(1).split(" ")(3))*/)}
    val packetsDS = lines.map{cad => Trace(cleasingOri(cad._2), cleasingDest(cad._2), cleasingProt(cad._2),5/*, checkBytes(cad._2.split("→")(1).split(" ")(3))*/)}
    
    //packetsDS.printSchema()
    /*
    val query = packetsDS.groupBy($"ori", $"des", $"pro").count().orderBy(-$"count")
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
		*/
    
    val query = packetsDS.toDF.withColumn("x", concat($"ori",  $"des", $"pro")/*+ ";" + $"des" + ";" +$"pro"*/ )
      .select($"ori" as "key", $"x" as "value")
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092,anotherhost:9092")
      .option("topic", "connect-test2")
      .option("checkpointLocation", "/tmp/kafkaSink")
      .start()

    query.awaitTermination()
    
    /*
    val query = packetsDS.withWatermark("timestamp", "10 minutes")
      .groupBy(
        window($"timestamp", "10 minutes", "5 minutes"),
        $"ori")
      .count().orderBy(-$"count").select($"ori" as "key", $"count" as "value")
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092,anotherhost:9092")
      .option("topic", "connect-test2")
      .option("checkpointLocation", "/tmp/kafkaSink")
      .start()

    query.awaitTermination()
    */
    //println("Needine.com\nNetwork Traffic Status V0.2")
  }
  
}
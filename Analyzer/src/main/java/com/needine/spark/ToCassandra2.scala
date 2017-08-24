package com.needine.spark


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.ForeachWriter
import com.datastax.spark.connector.cql.CassandraConnector
import com.needine.spark.Tables.Origin_By_IP_TCP
import com.needine.spark.Tables.Packet


object ToCassandra2 {
  
  def getBytes(arr: Array[String]):  Double ={
    if ((arr==null) ){
      0.0
    }else if (arr.size<4) {
      0.0
    }else {
      if (arr(3)=="") {
         0.0
      } else {
        if (arr(3) matches "[\\+\\-0-9.e]+") arr(3).toDouble
        else 0.0
      }
    }
  }
  
  def convert2Double(arr: Array[String]):Double={ 
    if(arr.length==4){
      arr(0).concat(arr(1).concat(arr(2).concat(arr(3)))).toDouble
    }else{
      0.0
    }
  }
    
  def main(args: Array[String]) = {
    
    val spark = SparkSession.builder
      .config("spark.master", "local[*]")
      .appName("StructuredStreamingDataToCassandra")
      .config("spark.cassandra.connection.host", "localhost")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.Encoders    
    import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
    import org.apache.spark.sql.execution.debug._
    import org.apache.spark.sql.functions._

    spark.sparkContext.setLogLevel("WARN")
        
    val conn = CassandraConnector.apply(spark.sparkContext.getConf)
    conn.withSessionDo { session =>
      Statements.createKeySpace(session)
    }
    println("Cassandra KeySpace Initialized!!")
       
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092,anotherhost:9092")
      .option("subscribe", "cleanedData")
      .load()
      
    val lines = df.selectExpr("CAST(timestamp AS TIMESTAMP)", "CAST(value AS STRING)")//.select($"timestamp", $"value").withColumn("unix_arrival", unix_timestamp($"timestamp")).withColumn("unix_time_now", unix_timestamp)
    
    val originIP_TCP = lines.select($"value").as[String].map(_.split(";")).filter(arr=> arr(2)=="TCP").map(arr => (arr(0), convert2Double(arr(0).split("\\.")))).map{t=>
        Origin_By_IP_TCP(t._1, t._2)
      }
    
    val packet_TCP = lines.select($"value").as[String].map(_.split(";")).filter(arr=> arr(2)=="TCP")
      .map(arr => (convert2Double(arr(0).split("\\.")), convert2Double(arr(1).split("\\.")), getBytes(arr)  ) )
      .map{t =>
        Packet(System.currentTimeMillis() / 1000L,t._1,t._2,t._3)
      } 
    
    

    // This Foreach sink writer writes the output to cassandra.
    val writer2 = new ForeachWriter[Tables.Origin_By_IP_TCP] {
      override def open(partitionId: Long, version: Long) = true
      override def process(value: Tables.Origin_By_IP_TCP) = {
        conn.withSessionDo { session =>
            session.execute(Statements.saveOriginByIP(value.ip, value.ref))
        }
      }
      override def close(errorOrNull: Throwable) = {}
    }

    val query2 = originIP_TCP.writeStream.queryName("StructuredStreamingDataToCassandra2").foreach(writer2).start
    //query2.awaitTermination()
    

    val writer = new ForeachWriter[Tables.Packet] {
      override def open(partitionId: Long, version: Long) = true
      override def process(value: Tables.Packet) = {
        conn.withSessionDo { session =>
          session.execute(Statements.savePacket(value.time, value.origen, value.destiny, value.bytes))
        }
      }
      override def close(errorOrNull: Throwable) = {}
    }

    val query = packet_TCP.writeStream.queryName("StructuredStreamingDataToCassandra3").foreach(writer).start

    //query.awaitTermination()
 
    // CHECK HOW TO CREATE a Streaming DS/DF
    /*     
    val df3 = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "origin_by_ip_tcp", "keyspace" -> "network_monitor" )).load()
    df3.show(false)
    df3.printSchema()
    
    val query3 = df3.select($"ip").writeStream
    .outputMode("append")
    .format("console")
    .start()
    query3.awaitTermination()
    */

    spark.stop()

    //Needine.com
    //Network Traffic Status V0.2
  }
  
}
package com.needine.spark

import org.apache.spark.sql.SparkSession
import com.datastax.spark.connector.cql.CassandraConnector

object ToCassandra {
  
  
    
  def main(args: Array[String]) = {
    /*
    val spark = SparkSession
      .builder()
      .appName("StructuredStreamingDataToCassandra")
      .config("spark.master", "local[*]")
      .getOrCreate()
   	*/
    
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
    

    val conn = CassandraConnector.apply(spark.sparkContext.getConf)

    conn.withSessionDo { session =>
      Statements.createKeySpace(session)
    }
    println("Cassandra KeySpace Initialized!!")

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

    
    
    /*
    val packets = lines
      //.withWatermark("timestamp", "20 seconds")
      .groupBy(window($"timestamp", "10 seconds"),$"value").count().withColumn("end", $"window.end")
      .select("value", "count", "end" )
      .selectExpr("CAST(value AS STRING)", "CAST(count AS Long)" ,"CAST(end AS TIMESTAMP)" ).withColumn("end2", unix_timestamp($"end"))
      .filter($"end2".gt(unix_timestamp - 20))
      .sort(desc("end"))
    
    packets.printSchema()
    */

    val cols = List("time", "bytes")
    val df2 = lines.toDF(cols: _*)
    //df2.printSchema()
    
    
    /*
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
    */
    
    //val ds = df2/*.select( $"time", $"bytes")*/.as[Tables.Packet]
    //ds.printSchema()
    /*
    // This Foreach sink writer writes the output to cassandra.
    import org.apache.spark.sql.ForeachWriter
    val writer = new ForeachWriter[Tables.Packet] {
      override def open(partitionId: Long, version: Long) = true
      override def process(value: Tables.Packet) = {
        conn.withSessionDo { session =>
          session.execute(Statements.savePacket(value.time, value.bytes))
        }
      }
      override def close(errorOrNull: Throwable) = {}
    }

    val query =
      ds.writeStream.queryName("StructuredStreamingDataToCassandra").foreach(writer).start

    query.awaitTermination()
    
    */
    spark.stop()
    
    
    //Needine.com
    //Network Traffic Status V0.2
  }
  
}
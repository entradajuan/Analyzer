package com.needine.spark


import org.apache.spark.sql.SparkSession
import com.datastax.spark.connector.cql.CassandraConnector
import com.needine.spark.Tables.Origin_By_IP_TCP


object ToCassandra2 {
  
  def convert2Int(arr: Array[String]):Double={ 
    //val arr = ip.split(".")
    //println(ip)
    //arr(0).concat(arr(1).concat(arr(2).concat(arr(3)))).toInt 
    //arr(0).toInt
    //"1231231231321".toDouble
    if(arr.length==4){
      arr(0).concat(arr(1).concat(arr(2).concat(arr(3)))).toDouble
    }else{
      0.0
    }
    // 
    
    //arr.length
  }
    
    
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
      .option("subscribe", "cleanedData")
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
    
    val originIP_TCP = lines.select($"value").as[String].map(_.split(";")).filter(arr=> arr(2)=="TCP").map(arr => (arr(0), convert2Int(arr(0).split("\\.")))).map{t=>
        /*
        if(t._1.split(".").length>0) {
          Origin_By_IP_TCP(t._1.split(".")(0), t._2)          
        }else{
          Origin_By_IP_TCP("AAA", t._2)
        }*/
        Origin_By_IP_TCP(t._1, t._2)
      }
    
    //val originIP_TCP2 = lines.select($"value").as[String].map(_.split(";")).filter(arr=> arr(2)=="TCP").map(arr => (arr(0), convert2Int(arr(0).split("."))))
    
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
    
    val ds = df2/*.select( $"time", $"bytes")*/.as[Tables.Packet]
    //ds.printSchema()
    
    // This Foreach sink writer writes the output to cassandra.
    import org.apache.spark.sql.ForeachWriter
    /*
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
    spark.stop()
    */

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
    query2.awaitTermination()

    
    spark.stop()

    
    
    
    //Needine.com
    //Network Traffic Status V0.2
  }
  
}
package com.needine.spark


import org.apache.spark._
import org.apache.spark.streaming.{StreamingContext, Seconds, Minutes, Time}
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import kafka.serializer.StringDecoder
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession
import java.util.function.ToDoubleFunction
import shapeless.ops.nat.ToInt

object MonitorClassic {
   val checkpointDirectory = "_1f6tfghDtg3" 
  
   def functionToCreateContext():StreamingContext = {
      val conf = new SparkConf().setAppName("Streaming KMeans Example").setMaster("local[*]")
      val ssc = new StreamingContext(conf, Seconds(2))
      //ssc.checkpoint(checkpointDirectory)   // set checkpoint directory
      ssc
   }
     
  
   def main (args: Array[String]):Unit ={
    
    val context = StreamingContext.getOrCreate(checkpointDirectory, functionToCreateContext _)
    
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    
    val topics = Array("connect-test", "test")
    val stream = KafkaUtils.createDirectStream[String, String](
      context,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    
    
    val connectTest = stream.filter(record => record.topic == "connect-test")
    val testSteam = stream.filter(record => record.topic == "test")
    
    connectTest.map(record => (record.topic, record.key, record.value)).print()


    context.start
    context.awaitTermination()  


    println("Hello world!!")
    
  }
  
}
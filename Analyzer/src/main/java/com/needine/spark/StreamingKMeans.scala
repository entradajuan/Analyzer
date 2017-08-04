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

object StreamingKMeans {
  
  val checkpointDirectory = "_1f467y8ujg3" 
  
  def functionToCreateContext():StreamingContext = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Streaming KMeans Example")
    val ssc = new StreamingContext(conf, Seconds(2))
    ssc.checkpoint(checkpointDirectory)   // set checkpoint directory
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
    
    val topics = Array("test", "training")
    val stream = KafkaUtils.createDirectStream[String, String](
      context,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    
    val trainSteam = stream.filter(record => record.topic == "training")
    val testSteam = stream.filter(record => record.topic == "test")
    //testSteam.map(record => (record.topic, record.key, record.value)).print()
    
    val trainingData = trainSteam.map(record => (record.key, record.value)).map(_._2).map(s => Vectors.dense(s.split(' ').map(_.toDouble)))
    //trainingData.print()
    val testData = testSteam.map(record => (record.key, record.value)).map(_._2).map(s => Vectors.dense(s.split(' ').map(_.toDouble)))

    /*
    //val testData = stream.map(record => (record.key, record.value)).map(_._2).map(LabeledPoint.parse)
    val testData = testSteam.map(record => (record.key, record.value)).map(_._2).map(_.split(";")).map{arr =>
      LabeledPoint(arr(0).toDouble, Vectors.dense(arr(1).split(' ').map(_.toDouble)))    
    }
    */
    val model = new StreamingKMeans()
      .setK(3)
      .setDecayFactor(0.05)
      .setRandomCenters(1, 0.0)
    
    model.trainOn(trainingData)
    val latestModel = model.latestModel()
    
    val res = model.predictOn(testData)
    //val res = model.predictOnValues(testData.map(lp => (lp.label, lp.features)))
    res.print()
    //val cl = clusterArray(0)
    //println(cl)
    //clusterArray.print()
    res.foreachRDD { rdd =>  
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._      
      val clusterArray = latestModel.clusterCenters 
      clusterArray.foreach { println }
      
    }
    
    context.start
    context.awaitTermination()  


    println("Hello world!!")
    
  }
}
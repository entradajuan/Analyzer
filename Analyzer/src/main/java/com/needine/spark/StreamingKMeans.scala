package com.needine.spark


import org.apache.spark._
import org.apache.spark.streaming.{StreamingContext, Seconds, Minutes, Time}
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import kafka.serializer.StringDecoder
import org.apache.kafka.common.serialization.StringDeserializer


import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.{Seconds, StreamingContext}


object StreamingKMeans {
  
  val checkpointDirectory = "_check3w4g45gf26" // Parece que aqui hay algun problema y hay que añadir un digito cada vez que se ejecuta

  
  def functionToCreateContext():StreamingContext = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Streaming KMeans Example")
    val ssc = new StreamingContext(conf, Seconds(2))
    
    ssc.checkpoint(checkpointDirectory)   // set checkpoint directory
    ssc
    
  }
  
  
  def updateFunc(values: Seq[Int], state: Option[Int]): Option[Int] = {
    val currentCount = values.sum
    val previousCount = state.getOrElse(0)
    Some(currentCount + previousCount)  
  }
  
  def main (args: Array[String]):Unit ={

    //val conf = new SparkConf().setMaster("local[*]").setAppName("KafkaWordCount")
    //val ssc = new StreamingContext(conf, Seconds(10))
    
    //val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")
    //val topics = List("test").toSet
    //val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics).map(_._2)
    
    val context = StreamingContext.getOrCreate(checkpointDirectory, functionToCreateContext _)
    
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    
    val topics = Array("test", "connect-test")
    val stream = KafkaUtils.createDirectStream[String, String](
      context,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    
    stream.map(record => (record.topic, record.key, record.value)).print()
    
    //val lines = stream.map(record => (record.key, record.value)).map(_._2)
    //val trainingData = stream.map(record => (record.key, record.value)).map(_._2).map(Vectors.parse)
    
    
    //val trainingData = ssc.textFileStream(args(0)).map(Vectors.parse)
    //val testData = ssc.textFileStream(args(1)).map(LabeledPoint.parse)
    
    /*
    val words = lines.flatMap { l => l.split(" ") }
  
    val pairs = words.map { x => (x, 1) }
    val wordsCount = pairs.reduceByKey(_ + _)
    
    //wordsCount.print()
    
    
    
    // Now I have to update the TOTAL results
    val globalCountStream = wordsCount.updateStateByKey(updateFunc)
    globalCountStream.print()
    */
    
    /*
    val model = new StreamingKMeans()
      .setK(3)
      .setDecayFactor(1.0)
      .setRandomCenters(5, 0.0)
    
    model.trainOn(trainingData)
    model.predictOnValues(testData.map(lp => (lp.label, lp.features))).print()
    */    
    
    
    
    context.start
    context.awaitTermination()  
    
    

  }
  
}
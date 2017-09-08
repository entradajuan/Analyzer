package com.needine.spark

import org.apache.spark.sql.SparkSession
import com.needine.spark.Tables.TCPPacket
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

object MachineLearning {
  
  def main(args: Array[String]) ={
    val spark = SparkSession.builder
      .config("spark.master", "local[*]")
      .appName("MLClusteringOverCassandra")
      .config("spark.cassandra.connection.host", "localhost")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.Encoders    
    import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
    import org.apache.spark.sql.execution.debug._
    import org.apache.spark.sql.functions._

    spark.sparkContext.setLogLevel("WARN")
    
    val df = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "tcp_packet_by_origen_destiny", "keyspace" -> "network_monitor" )).load()
    //df.show(false)
    //df.printSchema()
    
    val data = df.as[TCPPacket].map{ p =>
      val features = Array(/*p.origen.toDouble, p.destiny.toDouble, p.time.toDouble,*/ p.bytes.toDouble )
      LabeledPoint(0.0, Vectors.dense(features))
    }

    data.show(false)

    val Array(training, test) = data.randomSplit(Array(0.99995, 0.00005))
    
    /*
    //val scaler = new StandardScaler().setInputCol("features").setOutputCol("scaledFeatures").setWithMean(false).setWithStd(true).fit(rawData)
    val scaler = new MaxAbsScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures").fit(rawData)
    val data = scaler.transform(rawData)
    data.show(false)
    //data.describe().show()
    
    val Array(training, test) = data.randomSplit(Array(0.9, 0.1))
    
    println("scaled")
    training.select('scaledFeatures).distinct().show(false)
    println("Distinct:" + training.select('scaledFeatures).distinct().count())
    */
    /*
    val moni = df.select('bytes)
    moni.groupBy('bytes).count().where('count> 99).orderBy('bytes.desc_nulls_last).show
    
    
    val models = Seq(2, 3,  4).map{k =>
      (k, new KMeans().setK(k)/*.setFeaturesCol("scaledFeatures")*/.fit(training))
    }
    
    val evaluations = models.map{ t =>
      (t._1, t._2, t._2.computeCost(data) )
    }

    evaluations.foreach(println)
    println("over " + training.count() + " examples")
    */
    
    val kmeans = new KMeans().setK(3).setFeaturesCol("features")
    val model = kmeans.fit(training)
     
    val res = model.transform(test)
    res.show(false)
    
    model.clusterCenters.foreach { println }
    println(model.computeCost(test))
    

    
    
    
    
    
  }
  
  
}
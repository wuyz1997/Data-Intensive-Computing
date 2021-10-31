package sparkstreaming

import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession 
import org.apache.spark.sql.streaming.GroupState

case class percentage(key: String, per1: Double, per2: Double)
object KafkaSpark {
  def main(args: Array[String]) {
    // make a connection to Kafka 
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.kafka").setLevel(Level.WARN)
    
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("COVID")
      .getOrCreate()

    // read (key, value) pairs from kafka
    var df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("Subscribe", "covid")
      .load()

    import spark.implicits._
    // extract key-value frospark.implicits._m (null, "a, 1")
    val res = df.withColumn("_tmp", split(col("value"),"\\,")).select(
        $"_tmp".getItem(1).as("FirstDose"),
        $"_tmp".getItem(3).as("SecondDose"),
        $"_tmp".getItem(7).as("Population"),
         $"_tmp".getItem(8).as("key"),
        $"_tmp".getItem(9).as("TargetGroup"),
        $"_tmp".getItem(6).as("Region")
        ).where("TargetGroup = 'ALL' AND Region = key")
    // println(lines)
    println(res)


    // measure the average value for each key in a stateful manner
    import spark.implicits._
  
    def mappingFunc(key: String, values: Iterator[Row], state: GroupState[(Double,Double, Double)]): Iterator[percentage] = {
      var (sum1,sum2,popu) = state.getOption.getOrElse((0.0,0.0,0.0))
      values.foreach{ x=>
       // println(x)
        sum1 =  x.getString(0).toDouble + sum1 
        sum2 =  x.getString(1).toDouble + sum2 
        popu =  x.getString(2).toDouble 
      }
  
      state.update((sum1,sum2,popu))
      val per1 = sum1/popu
      val per2 = sum2/popu
      Iterator(percentage(key,per1,per2))
    }
    
    val result = res.groupByKey(x=>x.getString(3)).flatMapGroupsWithState(outputMode= org.apache.spark.sql.streaming.OutputMode.Append(),timeoutConf=org.apache.spark.sql.streaming.GroupStateTimeout.ProcessingTimeTimeout())(func = mappingFunc _)
    // val result = spark.sql("select _1 as key, _2 as value from tmp")



    // output datastream to console
    //val query = result.writeStream
      //.format("console")
      //.outputMode("append")
      //.start()
     val query = result.writeStream
      .format("json")
      .option("path","/home/osboxes/Project/task1/result1")
      .option("checkpointLocation","/checkpoint1")
      .outputMode("append")
      .start()


    query.awaitTermination()
  }
}

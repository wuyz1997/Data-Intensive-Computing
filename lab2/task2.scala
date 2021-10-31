package sparkstreaming

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

object KafkaSpark {
  def main(args: Array[String]) {
    // make a connection to Kafka 
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.kafka").setLevel(Level.WARN)
    
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Average")
      .getOrCreate()

    // read (key, value) pairs from kafka
    var df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("Subscribe", "avg")
      .load()

    import spark.implicits._
    // extract key-value frospark.implicits._m (null, "a, 1")
    val res = df.withColumn("_tmp", split(col("value"),"\\,")).select(
        $"_tmp".getItem(0).as("key"),
        $"_tmp".getItem(1).as("value")
        )
    // println(lines)
  

    // measure the average value for each key in a stateful manner
    def mappingFunc(key: String, values: Iterator[Row], state: GroupState[(Int, Double)]): (String, Double) = {
      var (cnt, sum) = state.getOption.getOrElse((0, 0.0))
      values.foreach{ x=>
       // println(x)
       sum =  x.getString(1).toDouble + sum  
        cnt = cnt + 1
      }

      state.update((cnt, sum))
      val avg_value = sum/cnt
      (key, avg_value)
    }

    val result = res.groupByKey(x=>x.getString(0)).mapGroupsWithState(func = mappingFunc _)
    // val result = spark.sql("select _1 as key, _2 as value from tmp")

    // output datastream to console
    val query = result.writeStream
      .format("console")
      .outputMode("update")
      .start()

    query.awaitTermination()
  }
}
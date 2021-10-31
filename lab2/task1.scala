package sparkstreaming

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

object KafkaSpark {
  def main(args: Array[String]) {
    // make a connection to Kafka and read (key, value) pairs from it
    val conf = new SparkConf().setMaster("local[2]").setAppName("Average")
    val ssc = new StreamingContext(conf, Seconds(1))
    val kafkaConf = Map(
      "metadata.broker.list" -> "localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "kafka-spark-streaming",
      "zookeeper.connection.timeout.ms" -> "1000")
    val topics = Set("avg")
    val messages = KafkaUtils.createDirectStream[
      String, String, StringDecoder, StringDecoder](
        ssc, kafkaConf, topics)
    
    // extract key-value from (null, "a, 1")
    val message = messages.map(x => x._2.split(","))
    val pairs = message.map(record => (record(0), record(1).toDouble)) 

    // measure the average value for each key in a stateful manner
    def mappingFunc(key: String, value: Option[Double], state: State[(Int, Double)]): (String, Double) = {
      val (count, sum) = state.getOption.getOrElse((0, 0.0))
      val next_sum = value.getOrElse(0.0) + sum 
      val next_count = count + 1
      state.update((next_count, next_sum))
      
      val avg_value = next_sum/next_count
      (key, avg_value)
    }

    val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _))
    stateDstream.print()

    ssc.checkpoint(".")
    ssc.start()
    ssc.awaitTermination()
  }
}

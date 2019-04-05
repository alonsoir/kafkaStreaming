package chapter9

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka._
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema
import org.apache.flink.util.NetUtils
import java.util.Properties
import org.apache.flink.api.common.functions._
import org.apache.flink.streaming.api.windowing.time.Time

object KafkaStreamWithFlink {

  def main(args: Array[String]): Unit = {

    /*obtain an execution enrionment*/
    val exec_env = StreamExecutionEnvironment.getExecutionEnvironment()

    /*provides kafka parameters
    val kafkaParameters = Map[String, String](
      "bootstrap.servers" -> "localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.AutoOffsetRest -> "kafka",
      "group.id" -> "consumerGroup",
      ConsumerConfig.AutoCommit.toString -> "false"
    )
    */

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    // only required for Kafka 0.8
    // properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", "consumerGroup")
    properties.setProperty("auto.offset.reset","latest")
    properties.setProperty("enable.auto.commit","false")
    properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")

    val kafkaParameters = Map[String, String](
      //ConsumerConfig.
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.GROUP_ID_CONFIG -> "consumerGroup",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
    )

    /*pass topic name*/
    val topics = "pharma-topic"
    /*define source, which is kafka in this case.
      need to provide the connector - flink-connector-kafka-0.10_2.11 in build.sbt file*/
    val consumerForOffset = new FlinkKafkaConsumer[String](topics, new SimpleStringSchema(), properties)
    /*read the offset from earliest*/
    consumerForOffset.setStartFromEarliest()
    val dataStream = exec_env.addSource(consumerForOffset)

    /*print raw stream*/
    dataStream.print()

    /*count the GDP per country in a window of 10 seconds
    val gdpPerCountry = dataStream
      .keyBy(0) //partition by country
      .timeWindow(Time.seconds(10)) //find the result based on time window of 10 seconds
      .apply(
      (val1, val2) => val2,
      (key, window, vals, c: Collector[(String, Long)]) => {
        if (vals.head != null) c.collect((vals.head.state, 1))
      }
    )


     */
    /*print the gdpPerCountry*/
    // gdpPerCountry.print

    /*trigger the program execution*/
    exec_env.execute("Kafka Window Stream WordCount")
  }
}
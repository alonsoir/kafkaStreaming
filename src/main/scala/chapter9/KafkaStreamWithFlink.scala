package chapter9

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka._
import org.apache.kafka.clients.consumer.ConsumerConfig


object KafkaStreamWithFlink {

  def main(args: Array[String]): Unit = {

    /*obtain an execution enrionment*/
    val exec_env = StreamExecutionEnvironment.getExecutionEnvironment()
    // processing time up to 200 ms per window
    exec_env .setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 8 thread
    //exec_env .setParallelism(8)

    exec_env.setMaxParallelism(1600)
    // by default is 100 ms
    exec_env.setBufferTimeout(80)

    /*
      You can see KafkaAndSparkStreaming in order how to configure kafka properties.
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

    /*pass topic name*/
    val topics = "pharma-topic"
    /*  define source, which is kafka in this case.
        need to provide the connector - flink-connector-kafka-0.10_2.11 in build.sbt file*/
    val consumerForOffset = new FlinkKafkaConsumer[String](topics, new SimpleStringSchema(), properties)
    /*read the offset from earliest*/
    consumerForOffset.setStartFromEarliest()
    val dataStream = exec_env.addSource(consumerForOffset)

    val windowSize=5
    val sliceSize=5


    /*print raw stream*/
    dataStream.print()



    //val countryWindow = dataStream.flatMap(_.split(",")).map(_,1).countWindow(windowSize, sliceSize).sum(0)
    //countryWindow.print
    /*
    count the GDP per country in a window of 10 seconds
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
package chapter9

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka._
import org.apache.kafka.clients.consumer.ConsumerConfig

/*case class to apply schema to the fields coming in input*/
case class PharmaEvent(country: String,
                       year: String,
                       pcnt_health_expend: String,
                       pcnt_gdp: String,
                       usd_capital_expend: String,
                       flag_codes: String,
                       total_expend: String)

case class Order(user: Long, product: String, amount: Int)

object KafkaStreamWithFlink {

  def main(args: Array[String]): Unit = {

    /*obtain an execution enrionment*/
    val exec_env = StreamExecutionEnvironment.getExecutionEnvironment

    // flink-table not present in flink 1.8. Not using until it pass through next stable release.
    // val tExec_env = TableEnvironment.getTableEnvironment(exec_env)

    /*
    val orderA = exec_env.fromCollection(Seq(
      Order(1L, "beer", 3),
      Order(1L, "diaper", 4),
      Order(3L, "rubber", 2))).toTable(tExec_env)

    val orderB = exec_env.fromCollection(Seq(
      Order(2L, "pen", 3),
      Order(2L, "rubber", 3),
      Order(4L, "beer", 1))).toTable(tExec_env)

    // union the two tables
    val result: DataStream[Order] = orderA.unionAll(orderB)
      .select('user, 'product, 'amount)
      .where('amount > 2)
      .toAppendStream[Order]

    result.print()
*/
    // processing time up to 200 ms per window
    exec_env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // Maximun num cores per thread
    exec_env.setParallelism(StreamExecutionEnvironment.getDefaultLocalParallelism)
    // by default is 100 ms
    exec_env.setBufferTimeout(80)

    /*
      You can see KafkaAndSparkStreaming in order how to configure kafka properties programmatically and via server.properties.
    */

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    // only required for Kafka 0.8
    // properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", "consumerGroup")
    properties.setProperty("auto.offset.reset", "latest")
    properties.setProperty("enable.auto.commit", "false")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    /*pass topic name*/
    val topics = "pharma-topic"
    /*  define source, which is kafka in this case.
        need to provide the connector - flink-connector-kafka-0.10_2.11 in build.sbt file*/
    val consumerForOffset = new FlinkKafkaConsumer[String](topics, new SimpleStringSchema(), properties)
    /*read the offset from earliest*/
    consumerForOffset.setStartFromEarliest()
    val dataStream = exec_env.addSource(consumerForOffset)

    /*print raw stream*/
    dataStream.print()

    // Ideally the best is to use sql in order to get data, but
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
    exec_env.execute("Kafka Stream With Flink")
  }

  def parseMap(line: String): PharmaEvent = {
    val record = line.substring(1, line.length - 1).split(",")
    //(record(0), record(1), record(2), record(3),record(4),record(5),record(7))
    PharmaEvent(record(0), record(1), record(2), record(3), record(4), record(5), record(7))
  }

}
package chapter9

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object KafkaSparkStreamingReceiver {

  def main(args: Array[String]) {

    Logger.getRootLogger.setLevel(Level.ERROR)

    val topics = Array("pharma-topic")

    val kafkaParameters = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.GROUP_ID_CONFIG -> "consumerGroup",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
    )
/*
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
*/
    /*create spark streaming context*/
    val sparkConf = new SparkConf().setAppName("KafkaAndSparkStreamingReceiver").setMaster("local[*]")
    val ssc =  new StreamingContext(sparkConf, Seconds(10))
    /*perform checkpoiting*/
    // in a production environment, use hdfs, hdfs://localhost:9000/chk-data/kafka/
    // in my laptop, i have to use a local folder /tmp/kafka
    ssc.checkpoint("/tmp/kafka")

    /*create stream using KafkaUtils and createStream*/
    //val rawStream = KafkaUtils.createDirectStream(ssc, zkQuorum, group, topics)
    val rawStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParameters))


    /*perform analytics*/
    rawStream.foreachRDD{rdd =>

      /*create SQL context*/
      val sqlContext = SparkSession.builder().getOrCreate().sqlContext

      /*convert rdd into dataframe by providing header information*/
      val data = rdd.map(_.value().split(",").to[List]).map(Utils.row)

      /*convert rdd into dataframe by providing header information*/
      val pharmaAnalyticsDF = sqlContext.createDataFrame(data, Utils.schema)

      /*create temporary table*/
      pharmaAnalyticsDF.registerTempTable("pharmaAnalytics")

      /*find total gdp spending per country*/
      val gdpByCountryDF = sqlContext.sql("select country, sum(pc_gdp) as total_gdp from pharmaAnalytics group by country")
      gdpByCountryDF.show(10,false)

      /*find total capital spending in USD per country in year 2015*/
      val spendingPerCapitaDF = sqlContext.sql("select country, time, sum(usd_cap) as spending_per_capita from pharmaAnalytics group by country,time having time=2015")
      spendingPerCapitaDF.show(10,false)

    }

    /*start the streaming job*/
    ssc.start()
    ssc.awaitTermination()
  }
}

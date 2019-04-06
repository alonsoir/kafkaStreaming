package chapter9

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.{SparkConf, TaskContext}

object KafkaAndSparkStreaming {

  /*main method*/
  def main(args: Array[String]) {
    /*logging at error level*/
    Logger.getRootLogger.setLevel(Level.ERROR)

    /*Since spark streaming is a consumer, we need to pass it topics*/
    val topics = Set("pharma-topic")
    /*pass kafka configuration details*/
    val kafkaParameters = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.GROUP_ID_CONFIG -> "consumerGroup",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
    )

    /*create spark configurations*/
    val sparkConf = new SparkConf().setAppName("KafkaAndSparkStreaming").setMaster("local[*]")
    /*create spark streaming context*/
    val ssc = new StreamingContext(sparkConf, Seconds(30))

    /*There are multiple ways to get data from kafka topic using KafkaUtils class.
     * One such method to get data using createDirectStream where this method pulls data using direct stream approach*/
    val rawStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParameters))

    /*Get metadata of the records and perform analytics on records*/
    rawStream.foreachRDD { rdd =>
      /*get offset ranges of partitions that will be used to get partition and offset information
       * and also this information will be used to commit the offset*/
      val rangeOfOffsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      /*get partition information*/
      rdd.foreachPartition { iter =>
        val metadata = rangeOfOffsets(TaskContext.get.partitionId)
        /*print topic, partition, fromoofset and lastoffset of each partition*/
        println(s"topic: ${metadata.topic} partition: ${metadata.partition} fromOffset: ${metadata.fromOffset} untilOffset: ${metadata.untilOffset}")
      } // rdd.foreachPartition
      /*using SQL on top of streams*/
      /*create SQL context*/
      val sqlContext = SparkSession.builder().getOrCreate().sqlContext

      val data = rdd.map(_.value().split(",").to[List]).map(Utils.row)

      /*convert rdd into dataframe by providing header information*/
      val pharmaAnalyticsDF = sqlContext.createDataFrame(data, Utils.schema)
      /*create temporary table*/
      pharmaAnalyticsDF.registerTempTable("pharmaAnalytics")

      /*find total gdp spending per country*/
      val gdpByCountryDF = sqlContext.sql("select country, sum(pc_gdp) as sum_pc_gdp from pharmaAnalytics group by country")
      gdpByCountryDF.show(100,false)

      sqlContext.sql("select country, time, pc_gdp, sum(pc_gdp) as sum_pc_gdp from pharmaAnalytics group by country,time,pc_gdp order by country").show(10,false)
      /*commit the offset after all the processing is completed*/
      rawStream.asInstanceOf[CanCommitOffsets].commitAsync(rangeOfOffsets)
    } // rdd.forEach

    ssc.start()
    ssc.awaitTermination()
  } // main
}


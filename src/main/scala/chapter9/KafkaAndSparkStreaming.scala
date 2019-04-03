package chapter9

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.{SparkConf, TaskContext}

object KafkaAndSparkStreaming {

  /*main method*/
  def main(args: Array[String]) {
    /*logging at warning level*/
    Logger.getRootLogger.setLevel(Level.WARN)

    /*Since spark streaming is a consumer, we need to pass it topics*/
    val topics = Set("pharma")
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
    // val rawStream = KafkaUtils.createDirectStream[String, String, StringDecoder,StringDecoder](ssc, kafkaParameters, topics)

    val rawStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParameters))

    /*gets raw data from topic, explode it, and break it into words */
    val records = rawStream.map(_.value())

    /*Get metadata of the records and perform analytics on records*/
    rawStream.foreachRDD { rdd =>
      /*get offset ranges of partitions that will be used to get partition and offset information
       * and also this information will be used to commit the offset*/
      val rangeOfOffsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      /*get partition information*/
      rdd.foreachPartition { iter =>
        val metadata = rangeOfOffsets(TaskContext.get.partitionId)
        /*print topic, partition, fromoofset and lastoffset of each partition*/
        println(s"${metadata.topic} ${metadata.partition} ${metadata.fromOffset} ${metadata.untilOffset}")
      }
      /*using SQL on top of streams*/
      /*create SQL context*/
      val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)

      /*convert rdd into dataframe by providing header information*/
      // val pharmaAnalytics = rdd.toDF("country","time","pc_healthxp","pc_gdp","usd_cap","flag_codes","total_spend")


      def dfSchema(columnNames: List[String]): StructType =
        StructType(
          Seq(
            StructField(name = "country", dataType = StringType, nullable = false),
            StructField(name = "time", dataType = IntegerType, nullable = false),
            StructField(name = "pc_healthxp", dataType = IntegerType, nullable = false),
            StructField(name = "pc_gdp", dataType = IntegerType, nullable = false),
            StructField(name = "usd_cap", dataType = IntegerType, nullable = false),
            StructField(name = "flag_codes", dataType = IntegerType, nullable = false),
            StructField(name = "total_spend", dataType = IntegerType, nullable = false)
          )
        )


      def row(line: List[String]): Row = Row(line(0), line(1).toInt, line(2).toInt, line(3).toInt, line(4).toInt, line(5).toInt, line(6).toInt)

      val schema = dfSchema(List("country", "time", "pc_healthxp", "pc_gdp", "usd_cap", "flag_codes", "total_spend"))

      val data = rdd.map(_.value().split(",").to[List]).map(row)
      //val data = rdd.map(_.split(",").to[List]).map(row)
      val pharmaAnalyticsDF = sqlContext.createDataFrame(data, schema)
      /*create temporary table*/
      pharmaAnalyticsDF.registerTempTable("pharmaAnalytics")

      /*find total gdp spending per country*/
      val gdpByCountryDF = sqlContext.sql("select country, sum(total_gdp) as total_gdp from pharmaAnalytics group by country")
      gdpByCountryDF.show(10)

      /*commit the offset after all the processing is completed*/
      rawStream.asInstanceOf[CanCommitOffsets].commitAsync(rangeOfOffsets)
    }
  }
}


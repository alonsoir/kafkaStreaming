package chapter9

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
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
    /*configure all the parameters for createStream*/
    //val zkQuorum = "localhost:2181, localhost:2182"
    //val group = "kafka-receiver-based"
    //val numThreads = "1"
    /*createStream takes topic name and number of thread needed to pull data in parallel*/
    val topics = Array("pharma-topic")

    //val topics = Set("pharma").map((_, numThreads.toInt)).toMap

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

    /*raw streams prints the data along with stream information*/
    // rawStream.print()

    /*get only streams of data*/
    val lines = rawStream.map(lines=>(lines.key(),lines.value()))

    Logger.getRootLogger.debug("INIT LINES")
    lines.print()
    Logger.getRootLogger.debug("END LINES")

    /*perform analytics*/
    rawStream.foreachRDD{rdd =>

      /*create SQL context*/
      val sqlContext = SparkSession.builder().getOrCreate().sqlContext
      // val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
      /*convert rdd into dataframe by providing header information*/
      def dfSchema(columnNames: List[String]): StructType =
      StructType(
        Seq(
          StructField(name = "country", dataType = StringType, nullable = false),
          StructField(name = "time", dataType = IntegerType, nullable = false),
          StructField(name = "pc_healthxp", dataType = FloatType, nullable = false),
          StructField(name = "pc_gdp", dataType = FloatType, nullable = false),
          StructField(name = "usd_cap", dataType = FloatType, nullable = false),
          StructField(name = "flag_codes", dataType = StringType, nullable = true),
          StructField(name = "total_spend", dataType = FloatType, nullable = false)
        )
      )

      def row(line: List[String]): Row = Row(line(0), line(1).toInt, line(2).toFloat, line(3).toFloat, line(4).toFloat, line(5), line(6).toFloat)

      val schema = dfSchema(List("country", "time", "pc_healthxp", "pc_gdp", "usd_cap", "flag_codes", "total_spend"))

      val data = rdd.map(_.value().split(",").to[List]).map(row)

      /*convert rdd into dataframe by providing header information*/
      val pharmaAnalyticsDF = sqlContext.createDataFrame(data, schema)



      // val pharmaAnalytics = rdd.toDF("country","time","pc_healthxp","pc_gdp","usd_cap","flag_codes","total_spend")
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

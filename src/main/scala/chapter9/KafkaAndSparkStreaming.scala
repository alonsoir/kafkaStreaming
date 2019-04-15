package chapter9

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.{SparkConf, TaskContext}

/***
  * This class tries to show the first way to get data streaming between spark and kafka 0.10.X.
  * It is the recommended way to use, i.e. instantiate the static createDirectStream method,
  * because it is parallelizable and therefore gives more performance.
  *
  * In previous versions, 0.8.X, there was another way that today is completely discouraged, so much so that currently can not be used in version 0.10.X.
  */
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
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
      // DEFAULT values. session.timeout.ms, 10000 in Kafka +0.10.1 and 30000 in Kafka -0.10.0
      ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG -> "10000",
      // This can be incremented in order to avoid generating consumers losses,
      // only if it is in the range of the properties group.min.session.timeout.ms and group.max.session.timeout.ms
      ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG -> "3000",
      // It must be ⅓ of the time of session.timeout.ms and you should increase it as incrementing session.timeout.ms maintaining the proportion.
      ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG -> "300000",
      // The maximum time between polls before generating the consumer loss.    request.timeout.ms, 305000
      ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG -> "305000",
      // It must be bigger than max.poll.interval.ms. connections.max.idle.ms, 540000
      ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG -> "540000"
      // Parameters to be configured in Kafka brokers. num.network.threads, 3 TODO!
    )

    /*
    Making partitions in Kafka over the topics which are going to be consumed is very important,
    hence this will allow you to parallelize the reception of the events in different Spark executors.
    Creating a relation partition-executor will make every executor receive a chunk of data from the Kafka topic.
    Below we can see how to build a topic with three partitions and the configuration needed to run the Spark job
    with three executors using two cores each and 2GB of RAM.

    ./bin/kafka-topics.sh --create --replication-factor 1 --partitions 3 --zookeeper zookeeper:2181 --topic pharma-topic

    conf.set("spark.cores.max", 6)
    conf.set("spark.executor.cores", 2)
    conf.set("spark.executor.memory", 2GB)

    https://www.stratio.com/blog/optimizing-spark-streaming-applications-apache-kafka/

    https://stackoverflow.com/questions/26562033/how-to-set-apache-spark-executor-memory

    */

    /*create spark configurations*/
    val sparkConf = new SparkConf()
                        .setAppName("KafkaAndSparkStreaming")
                        .setMaster("local[*]")
                        // 3 partitions, 3 executors using two cores each with 2GB ram.
                        // One executor per partition. spark.cores.max equal to number partitions multiplied by spark.executor.cores
                        .set("spark.cores.max","6")
                        .set("spark.executor.cores","2")
                        .set("spark.executor.memory","2g")
                        // Optimize the executor election when Spark compute one task, this have direct impact into the Scheduling Delay.
                        .set("spark.locality.wait","100")
                        // Activating the BackPressure resulted in the stable performance of a streaming application.
                        // You should activate it in Spark production environments with Kafka
                        .set("spark.streaming.backpressure.enabled","true")
                        // Probably, the most important configuration parameter assigned to the Kafka consumer.
                        // This function is very delicate because it is the one which returns the records to Spark requested by Kafka by a .seek.
                        // If after two attempts there is a timeout, the Task is FAILED and sent to another Spark executor causing a delay in the streaming window.
                        // If the timeout is too low and the Kafka brokers need more time to answer there will be many “TASK FAILED”s,
                        // which causes a delay in the preparation of the assigned tasks into the Executors.
                        // However, if this timeout is too high the Spark executor wastes a lot of time doing nothing
                        // and produces large delays (pollTimeout + task scheduling in the new executor).
                        //The default value of the Spark version 2.x. was 512ms, which could be a good start.
                        // If the Spark jobs cause many “TASK FAILED”s you would need to RAISE that value and investigate why the Kafka brokers
                        // took so long to send the records to the poll.
                        .set("spark.streaming.kafka.consumer.poll.ms","512")
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
      /*
         Get offset ranges of partitions that will be used to get partition and offset information
         and also this information will be used to commit the offset.
         The offsets indicate where the groupld assigned to the Spark consumer is reading from.
         This is very important because it guarantees the HA during the streaming process and avoids losing data if there is an error.
       */
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
      /*
        Commit the offset after all the processing is completed.
        Using the new API to manage the offsets is not necessary neither to make the implementation
        nor to activate the checkpoint when associated with low performance moments.
        With the following code lines you can keep the offsets in Kafka at any time because the data has been processed correctly
       */
      rawStream.asInstanceOf[CanCommitOffsets].commitAsync(rangeOfOffsets)
    } // rdd.forEach

    ssc.start()
    ssc.awaitTermination()
  } // main
}

